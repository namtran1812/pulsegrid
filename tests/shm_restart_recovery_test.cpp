#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>

#include <gtest/gtest.h>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include "pulsegrid/frame_codec.hpp"
#include "pulsegrid/shm_queue.hpp"
#include "pulsegrid/shm_region.hpp"
#include "pulsegrid/state_store.hpp"
#include "pulsegrid/transport_frame.hpp"

namespace {

constexpr std::size_t kCapacity = 8;

constexpr std::uint64_t kEpochA =
    0xA000000000000001ULL;

constexpr std::uint64_t kEpochB =
    0xB000000000000001ULL;

using Queue = pulsegrid::ShmQueue<
    kCapacity,
    pulsegrid::TransportFrame
>;

pulsegrid::Update make_update(
    std::uint32_t row,
    std::uint64_t sequence,
    std::uint64_t value
) {
    return {
        .table_id = 1,
        .row_id = row,
        .column_id = 1,
        .type = pulsegrid::ValueType::UInt64,
        .sequence = sequence,
        .payload = value
    };
}

std::string unique_shm_name(
    const char* suffix
) {
    const auto nonce =
        std::chrono::steady_clock::now()
            .time_since_epoch()
            .count();

    return "/pgr_" +
           std::to_string(
               static_cast<long long>(::getpid())
           ) +
           "_" +
           std::to_string(
               static_cast<long long>(nonce)
           ) +
           "_" +
           suffix;
}

void write_byte_or_exit(
    int fd,
    char value,
    int exit_code
) {
    if (
        ::write(fd, &value, sizeof(value)) !=
        sizeof(value)
    ) {
        std::_Exit(exit_code);
    }
}

char read_byte_or_fail(int fd) {
    char value = 0;

    EXPECT_EQ(
        ::read(fd, &value, sizeof(value)),
        sizeof(value)
    );

    return value;
}

void apply_frame(
    pulsegrid::FrameDecoder& decoder,
    pulsegrid::StateStore& store,
    const pulsegrid::TransportFrame& frame
) {
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
}

pulsegrid::TransportFrame pop_frame(
    Queue& queue
) {
    pulsegrid::TransportFrame frame{};

    for (;;) {
        if (queue.try_pop(frame)) {
            return frame;
        }

        ::sched_yield();
    }
}

} // namespace

TEST(
    ShmRestartRecovery,
    CrashRestartRequiresExplicitEpochRecovery
) {
    /*
     * Incarnation A.
     *
     * The child is the producer. It creates and
     * initializes the SHM queue, publishes seq 1..3,
     * then waits indefinitely. The parent consumes
     * those publications and kills the producer.
     */
    const std::string name_a =
        unique_shm_name("a");

    int ready_a[2]{};

    ASSERT_EQ(::pipe(ready_a), 0);

    const pid_t producer_a = ::fork();

    ASSERT_NE(producer_a, -1);

    if (producer_a == 0) {
        ::close(ready_a[0]);

        try {
            pulsegrid::ShmRegion region{
                name_a,
                Queue::mapped_size(),
                pulsegrid::ShmRegion::Mode::Create
            };

            auto queue =
                Queue::initialize(
                    region.data(),
                    kEpochA
                );

            for (
                std::uint64_t sequence = 1;
                sequence <= 3;
                ++sequence
            ) {
                const auto frame =
                    pulsegrid::encode_delta(
                        make_update(
                            static_cast<std::uint32_t>(
                                sequence
                            ),
                            sequence,
                            sequence * 100
                        )
                    );

                while (!queue.try_push(frame)) {
                    ::sched_yield();
                }
            }

            write_byte_or_exit(
                ready_a[1],
                1,
                11
            );

            ::close(ready_a[1]);

            /*
             * Simulate a live producer that has
             * stopped making progress. The parent
             * terminates this process below.
             */
            for (;;) {
                ::pause();
            }
        }
        catch (...) {
            std::_Exit(12);
        }
    }

    ::close(ready_a[1]);

    ASSERT_EQ(read_byte_or_fail(ready_a[0]), 1);

    ::close(ready_a[0]);

    pulsegrid::StateStore store;

    /*
     * Keep this frame as a delayed publication from
     * incarnation A. We will replay it only after
     * recovery to epoch B.
     */
    pulsegrid::TransportFrame delayed_a{};

    {
        pulsegrid::ShmRegion region{
            name_a,
            Queue::mapped_size(),
            pulsegrid::ShmRegion::Mode::Open
        };

        auto queue =
            Queue::attach(region.data());

        ASSERT_EQ(
            queue.publication_epoch(),
            kEpochA
        );

        pulsegrid::FrameDecoder decoder(
            queue.publication_epoch()
        );

        apply_frame(
            decoder,
            store,
            pop_frame(queue)
        );

        apply_frame(
            decoder,
            store,
            pop_frame(queue)
        );

        apply_frame(
            decoder,
            store,
            pop_frame(queue)
        );

        /*
         * A delayed A:4 is intentionally retained
         * outside the queue. It represents an old
         * publication that arrives after recovery.
         */
        delayed_a =
            pulsegrid::encode_delta(
                make_update(
                    4,
                    4,
                    400
                )
            );

        ASSERT_TRUE(store.epoch());
        EXPECT_EQ(*store.epoch(), kEpochA);

        ASSERT_TRUE(store.last_sequence());
        EXPECT_EQ(*store.last_sequence(), 3U);

        ASSERT_EQ(
            ::kill(producer_a, SIGKILL),
            0
        );

        int status = 0;

        ASSERT_EQ(
            ::waitpid(
                producer_a,
                &status,
                0
            ),
            producer_a
        );

        ASSERT_TRUE(WIFSIGNALED(status));
        EXPECT_EQ(WTERMSIG(status), SIGKILL);

        /*
         * Producer A cannot perform graceful
         * cleanup after SIGKILL. The surviving
         * process owns stale-name cleanup.
         */
        region.unlink();
    }

    /*
     * Incarnation B uses a different SHM object and
     * a different publication epoch. Its sequence
     * stream starts again at 1.
     */
    const std::string name_b =
        unique_shm_name("b");

    int ready_b[2]{};
    int release_b[2]{};

    ASSERT_EQ(::pipe(ready_b), 0);
    ASSERT_EQ(::pipe(release_b), 0);

    const pid_t producer_b = ::fork();

    ASSERT_NE(producer_b, -1);

    if (producer_b == 0) {
        ::close(ready_b[0]);
        ::close(release_b[1]);

        try {
            pulsegrid::ShmRegion region{
                name_b,
                Queue::mapped_size(),
                pulsegrid::ShmRegion::Mode::Create
            };

            auto queue =
                Queue::initialize(
                    region.data(),
                    kEpochB
                );

            /*
             * B:1 is deliberately published before
             * recovery. Applying it directly to the
             * epoch-A StateStore must fail.
             */
            const auto first =
                pulsegrid::encode_delta(
                    make_update(
                        100,
                        1,
                        1000
                    )
                );

            while (!queue.try_push(first)) {
                ::sched_yield();
            }

            write_byte_or_exit(
                ready_b[1],
                1,
                21
            );

            ::close(ready_b[1]);

            char release = 0;

            if (
                ::read(
                    release_b[0],
                    &release,
                    sizeof(release)
                ) != sizeof(release)
            ) {
                std::_Exit(22);
            }

            ::close(release_b[0]);

            /*
             * Recovery snapshot below establishes
             * B at watermark 1. Normal publication
             * therefore resumes at B:2.
             */
            const auto second =
                pulsegrid::encode_delta(
                    make_update(
                        200,
                        2,
                        2000
                    )
                );

            while (!queue.try_push(second)) {
                ::sched_yield();
            }

            std::_Exit(0);
        }
        catch (...) {
            std::_Exit(23);
        }
    }

    ::close(ready_b[1]);
    ::close(release_b[0]);

    ASSERT_EQ(read_byte_or_fail(ready_b[0]), 1);

    ::close(ready_b[0]);

    {
        pulsegrid::ShmRegion region{
            name_b,
            Queue::mapped_size(),
            pulsegrid::ShmRegion::Mode::Open
        };

        auto queue =
            Queue::attach(region.data());

        ASSERT_EQ(
            queue.publication_epoch(),
            kEpochB
        );

        pulsegrid::FrameDecoder decoder_b(
            queue.publication_epoch()
        );

        const auto first_b =
            pop_frame(queue);

        const auto decoded_first_b =
            decoder_b.consume(first_b);

        ASSERT_TRUE(decoded_first_b.delta);
        ASSERT_EQ(
            decoded_first_b.delta->epoch,
            kEpochB
        );

        /*
         * Merely observing a new SHM epoch does not
         * authorize mutation of old state.
         */
        EXPECT_THROW(
            store.apply(
                decoded_first_b.delta->epoch,
                decoded_first_b.delta->update
            ),
            pulsegrid::EpochMismatch
        );

        ASSERT_TRUE(store.epoch());
        EXPECT_EQ(*store.epoch(), kEpochA);

        ASSERT_TRUE(store.last_sequence());
        EXPECT_EQ(*store.last_sequence(), 3U);

        /*
         * Explicit recovery is the only transition.
         *
         * Model the recovery/control plane with a
         * snapshot supplied out-of-band. This keeps
         * snapshot transfer off the normal 64-byte
         * publication fast path.
         */
        const pulsegrid::Snapshot recovery{
            .epoch = kEpochB,
            .sequence = 1,
            .updates = {
                make_update(
                    100,
                    1,
                    1000
                )
            }
        };

        store =
            pulsegrid::StateStore::restore(
                recovery
            );

        ASSERT_TRUE(store.epoch());
        EXPECT_EQ(*store.epoch(), kEpochB);

        ASSERT_TRUE(store.last_sequence());
        EXPECT_EQ(*store.last_sequence(), 1U);

        const auto recovered_cell =
            store.get(1, 100, 1);

        ASSERT_TRUE(recovered_cell);

        EXPECT_EQ(
            std::get<std::uint64_t>(
                recovered_cell->value
            ),
            1000U
        );

        /*
         * Now allow B to resume from the recovery
         * watermark.
         */
        const char release = 1;

        ASSERT_EQ(
            ::write(
                release_b[1],
                &release,
                sizeof(release)
            ),
            sizeof(release)
        );

        ::close(release_b[1]);

        apply_frame(
            decoder_b,
            store,
            pop_frame(queue)
        );

        ASSERT_TRUE(store.last_sequence());
        EXPECT_EQ(*store.last_sequence(), 2U);

        const auto continued_cell =
            store.get(1, 200, 1);

        ASSERT_TRUE(continued_cell);

        EXPECT_EQ(
            std::get<std::uint64_t>(
                continued_cell->value
            ),
            2000U
        );

        int status = 0;

        ASSERT_EQ(
            ::waitpid(
                producer_b,
                &status,
                0
            ),
            producer_b
        );

        ASSERT_TRUE(WIFEXITED(status));
        EXPECT_EQ(WEXITSTATUS(status), 0);

        region.unlink();
    }

    /*
     * Finally replay a delayed publication from A.
     * A decoder correctly identifies it as epoch A,
     * but the recovered store is now epoch B.
     */
    pulsegrid::FrameDecoder delayed_decoder_a(
        kEpochA
    );

    const auto delayed =
        delayed_decoder_a.consume(
            delayed_a
        );

    ASSERT_TRUE(delayed.delta);
    EXPECT_EQ(delayed.delta->epoch, kEpochA);

    EXPECT_THROW(
        store.apply(
            delayed.delta->epoch,
            delayed.delta->update
        ),
        pulsegrid::EpochMismatch
    );

    ASSERT_TRUE(store.epoch());
    EXPECT_EQ(*store.epoch(), kEpochB);

    ASSERT_TRUE(store.last_sequence());
    EXPECT_EQ(*store.last_sequence(), 2U);

    EXPECT_FALSE(store.get(1, 4, 1));

    const auto b1 = store.get(1, 100, 1);
    const auto b2 = store.get(1, 200, 1);

    ASSERT_TRUE(b1);
    ASSERT_TRUE(b2);

    EXPECT_EQ(
        std::get<std::uint64_t>(b1->value),
        1000U
    );

    EXPECT_EQ(
        std::get<std::uint64_t>(b2->value),
        2000U
    );
}
