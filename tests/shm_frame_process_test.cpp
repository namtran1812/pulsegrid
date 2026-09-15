#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <sys/wait.h>
#include <unistd.h>

#include "pulsegrid/backpressure_publisher.hpp"
#include "pulsegrid/frame_codec.hpp"
#include "pulsegrid/shm_queue.hpp"
#include "pulsegrid/shm_region.hpp"
#include "pulsegrid/state_store.hpp"
#include "pulsegrid/transport_frame.hpp"

namespace {

constexpr std::size_t kCapacity = 8;
constexpr std::uint64_t kPublicationEpoch =
    0xA11CE2026ULL;

using Queue = pulsegrid::ShmQueue<
    kCapacity,
    pulsegrid::TransportFrame
>;

pulsegrid::Update make_update(
    std::uint32_t row,
    std::uint16_t column,
    std::uint64_t sequence,
    std::uint64_t value
) {
    return {
        .table_id = 1,
        .row_id = row,
        .column_id = column,
        .type = pulsegrid::ValueType::UInt64,
        .sequence = sequence,
        .payload = value
    };
}

std::string unique_shm_name() {
    const auto nonce =
        std::chrono::steady_clock::now()
            .time_since_epoch()
            .count();

    return "/pgf_" +
           std::to_string(
               static_cast<long long>(::getpid())
           ) +
           "_" +
           std::to_string(
               static_cast<long long>(nonce)
           );
}



} // namespace

TEST(
    ShmFrameProcess,
    ReconstructsStateAcrossBackpressureBoundary
) {
    const std::string name = unique_shm_name();

    pulsegrid::ShmRegion region{
        name,
        Queue::mapped_size(),
        pulsegrid::ShmRegion::Mode::Create
    };

    auto producer_queue =
        Queue::initialize(
            region.data(),
            kPublicationEpoch
        );

    EXPECT_EQ(
        producer_queue.publication_epoch(),
        kPublicationEpoch
    );

    /*
     * Gate the child with a pipe. This lets the
     * parent deterministically fill the SHM queue
     * before the consumer begins popping frames.
     */
    int start_pipe[2]{};
    int ready_pipe[2]{};

    ASSERT_EQ(::pipe(start_pipe), 0);
    ASSERT_EQ(::pipe(ready_pipe), 0);

    const pid_t child = ::fork();

    ASSERT_NE(child, -1);

    if (child == 0) {
        ::close(start_pipe[1]);
        ::close(ready_pipe[0]);

        try {
            pulsegrid::ShmRegion child_region{
                name,
                Queue::mapped_size(),
                pulsegrid::ShmRegion::Mode::Open
            };

            auto consumer_queue =
                Queue::attach(child_region.data());

            consumer_queue.mark_consumer_attached();

            const char ready = 1;

            if (
                ::write(
                    ready_pipe[1],
                    &ready,
                    sizeof(ready)
                ) != sizeof(ready)
            ) {
                std::_Exit(2);
            }

            ::close(ready_pipe[1]);

            char start = 0;

            if (
                ::read(
                    start_pipe[0],
                    &start,
                    sizeof(start)
                ) != sizeof(start)
            ) {
                std::_Exit(3);
            }

            ::close(start_pipe[0]);

            pulsegrid::FrameDecoder decoder(
                consumer_queue.publication_epoch()
            );
            pulsegrid::StateStore store;

            constexpr std::uint32_t
                final_sequence = 14;

            while (true) {
                pulsegrid::TransportFrame frame{};

                if (!consumer_queue.try_pop(frame)) {
                    if (
                        consumer_queue
                            .consumer_complete()
                    ) {
                        std::_Exit(3);
                    }

                    std::this_thread::yield();
                    continue;
                }

                const auto result =
                    decoder.consume(frame);

                if (result.delta) {
                    store.apply(
                        result.delta->epoch,
                        result.delta->update
                    );
                }

                if (result.coalesced) {
                    store.apply_coalesced(
                        result.coalesced->epoch,
                        result.coalesced->batch
                    );
                }

                if (
                    store.last_sequence() &&
                    *store.last_sequence() ==
                        final_sequence
                ) {
                    break;
                }
            }

            if (decoder.in_batch()) {
                std::_Exit(4);
            }

            if (
                !store.last_sequence() ||
                *store.last_sequence() !=
                    final_sequence
            ) {
                std::_Exit(5);
            }

            /*
             * seq 1..8 were ordinary deltas.
             */
            for (
                std::uint64_t sequence = 1;
                sequence <= 8;
                ++sequence
            ) {
                const auto cell =
                    store.get(
                        1,
                        sequence,
                        1
                    );

                if (
                    !cell ||
                    std::get<std::uint64_t>(
                        cell->value
                    ) != sequence * 10
                ) {
                    std::_Exit(6);
                }
            }

            /*
             * seq 9..13 occurred while the queue
             * was saturated. They repeatedly
             * update only two cells:
             *
             *   row 100 -> seq 13 -> value 1300
             *   row 200 -> seq 12 -> value 1200
             *
             * The consumer should see only the
             * latest value for each cell while
             * advancing the global watermark
             * through sequence 13.
             */
            const auto row100 =
                store.get(1, 100, 1);

            const auto row200 =
                store.get(1, 200, 1);

            if (
                !row100 ||
                std::get<std::uint64_t>(
                    row100->value
                ) != 1300 ||
                row100->sequence != 13
            ) {
                std::_Exit(7);
            }

            if (
                !row200 ||
                std::get<std::uint64_t>(
                    row200->value
                ) != 1200 ||
                row200->sequence != 12
            ) {
                std::_Exit(8);
            }

            /*
             * seq 14 is published after the
             * coalesced catch-up and proves
             * ordinary sequencing resumes.
             */
            const auto final_cell =
                store.get(1, 300, 1);

            if (
                !final_cell ||
                std::get<std::uint64_t>(
                    final_cell->value
                ) != 1400 ||
                final_cell->sequence != 14
            ) {
                std::_Exit(9);
            }

            consumer_queue.mark_consumer_complete();

            std::_Exit(0);
        }
        catch (...) {
            std::_Exit(10);
        }
    }

    ::close(start_pipe[0]);
    ::close(ready_pipe[1]);

    char ready = 0;

    ASSERT_EQ(
        ::read(
            ready_pipe[0],
            &ready,
            sizeof(ready)
        ),
        sizeof(ready)
    );

    ASSERT_EQ(ready, 1);

    ::close(ready_pipe[0]);

    pulsegrid::BackpressurePublisher<
        kCapacity
    > publisher{producer_queue};

    /*
     * Capacity 8 can hold exactly eight delta
     * frames. The child is attached but gated,
     * so these publications deterministically
     * fill the queue.
     */
    for (
        std::uint64_t sequence = 1;
        sequence <= 8;
        ++sequence
    ) {
        ASSERT_TRUE(
            publisher.publish(
                make_update(
                    sequence,
                    1,
                    sequence,
                    sequence * 10
                )
            )
        );
    }

    /*
     * Queue is full. seq 9 must enter the
     * publisher's pending/coalescing state.
     */
    EXPECT_FALSE(
        publisher.publish(
            make_update(
                100,
                1,
                9,
                900
            )
        )
    );

    /*
     * Once pending state exists, newer updates
     * cannot leapfrog it even if queue state
     * later changes.
     */
    EXPECT_FALSE(
        publisher.publish(
            make_update(
                200,
                1,
                10,
                1000
            )
        )
    );

    EXPECT_FALSE(
        publisher.publish(
            make_update(
                100,
                1,
                11,
                1100
            )
        )
    );

    EXPECT_FALSE(
        publisher.publish(
            make_update(
                200,
                1,
                12,
                1200
            )
        )
    );

    EXPECT_FALSE(
        publisher.publish(
            make_update(
                100,
                1,
                13,
                1300
            )
        )
    );

    EXPECT_EQ(publisher.pending_cells(), 2);

    /*
     * Begin + 2 cells + End = four frames.
     * No room exists yet, so the atomic batch
     * enqueue must fail without losing pending
     * state.
     */
    EXPECT_FALSE(publisher.try_flush());
    EXPECT_EQ(publisher.pending_cells(), 2);

    /*
     * Release the child. It can now consume the
     * original eight deltas and create enough
     * space for the four-frame coalesced batch.
     */
    const char start = 1;

    ASSERT_EQ(
        ::write(
            start_pipe[1],
            &start,
            sizeof(start)
        ),
        sizeof(start)
    );

    ::close(start_pipe[1]);

    constexpr auto flush_timeout =
        std::chrono::seconds(5);

    const auto flush_deadline =
        std::chrono::steady_clock::now() +
        flush_timeout;

    bool flushed = false;

    while (
        std::chrono::steady_clock::now() <
        flush_deadline
    ) {
        if (publisher.try_flush()) {
            flushed = true;
            break;
        }

        std::this_thread::yield();
    }

    ASSERT_TRUE(flushed);
    EXPECT_EQ(publisher.pending_cells(), 0);

    /*
     * Publish seq 14 only after seq 9..13 has
     * been atomically committed to the queue.
     */
    constexpr auto publish_timeout =
        std::chrono::seconds(5);

    const auto publish_deadline =
        std::chrono::steady_clock::now() +
        publish_timeout;

    bool final_published = false;

    while (
        std::chrono::steady_clock::now() <
        publish_deadline
    ) {
        if (
            publisher.publish(
                make_update(
                    300,
                    1,
                    14,
                    1400
                )
            )
        ) {
            final_published = true;
            break;
        }

        /*
         * If publication encounters temporary
         * backpressure, publish() retains seq 14.
         * Flush it instead of publishing the same
         * sequence again.
         */
        if (publisher.pending_cells() != 0) {
            while (
                std::chrono::steady_clock::now() <
                publish_deadline
            ) {
                if (publisher.try_flush()) {
                    final_published = true;
                    break;
                }

                std::this_thread::yield();
            }

            break;
        }
    }

    ASSERT_TRUE(final_published);

    int status = 0;

    ASSERT_EQ(
        ::waitpid(child, &status, 0),
        child
    );

    EXPECT_TRUE(WIFEXITED(status));

    if (WIFEXITED(status)) {
        EXPECT_EQ(WEXITSTATUS(status), 0);
    }

    EXPECT_TRUE(
        producer_queue.consumer_complete()
    );

    region.unlink();
}
