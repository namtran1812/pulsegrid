#include <cstddef>
#include <cstdint>
#include <memory>

#include <gtest/gtest.h>

#include "pulsegrid/backpressure_publisher.hpp"
#include "pulsegrid/frame_codec.hpp"
#include "pulsegrid/state_store.hpp"

namespace {

pulsegrid::Update make_update(
    std::uint32_t row,
    std::uint64_t sequence,
    std::uint64_t payload
) {
    return {
        .table_id = 1,
        .row_id = row,
        .column_id = 1,
        .type =
            pulsegrid::ValueType::UInt64,
        .sequence = sequence,
        .payload = payload
    };
}

template <typename Queue>
class QueueMemory {
public:
    QueueMemory() {
        memory_ = ::operator new(
            Queue::mapped_size(),
            std::align_val_t{
                pulsegrid::kShmCacheLine
            }
        );
    }

    ~QueueMemory() {
        ::operator delete(
            memory_,
            std::align_val_t{
                pulsegrid::kShmCacheLine
            }
        );
    }

    void* get() noexcept {
        return memory_;
    }

private:
    void* memory_{nullptr};
};

} // namespace

TEST(BackpressurePublisher, PublishesDirectlyWhenQueueHasRoom) {
    using Publisher =
        pulsegrid::BackpressurePublisher<8>;

    using Queue = Publisher::Queue;

    QueueMemory<Queue> memory;
    auto queue =
        Queue::initialize(memory.get());

    Publisher publisher(queue);

    EXPECT_TRUE(
        publisher.publish(
            make_update(1, 1, 10)
        )
    );

    EXPECT_FALSE(
        publisher.backpressured()
    );

    pulsegrid::TransportFrame frame{};

    ASSERT_TRUE(queue.try_pop(frame));

    EXPECT_EQ(
        frame.kind,
        pulsegrid::FrameKind::Delta
    );

    EXPECT_EQ(
        frame.update.sequence,
        1
    );
}

TEST(BackpressurePublisher, CoalescesWhenQueueIsFull) {
    using Publisher =
        pulsegrid::BackpressurePublisher<4>;

    using Queue = Publisher::Queue;

    QueueMemory<Queue> memory;
    auto queue =
        Queue::initialize(memory.get());

    // Fill all four slots.
    for (std::uint64_t sequence = 1;
         sequence <= 4;
         ++sequence) {
        ASSERT_TRUE(
            queue.try_push(
                pulsegrid::encode_delta(
                    make_update(
                        sequence,
                        sequence,
                        sequence
                    )
                )
            )
        );
    }

    Publisher publisher(queue);

    EXPECT_FALSE(
        publisher.publish(
            make_update(1, 5, 50)
        )
    );

    EXPECT_FALSE(
        publisher.publish(
            make_update(1, 6, 60)
        )
    );

    EXPECT_TRUE(
        publisher.backpressured()
    );

    // Same cell: latest value wins.
    EXPECT_EQ(
        publisher.pending_cells(),
        1
    );

    EXPECT_EQ(
        publisher.pending_watermark(),
        6
    );
}

TEST(BackpressurePublisher, FailedFlushPreservesPendingState) {
    using Publisher =
        pulsegrid::BackpressurePublisher<4>;

    using Queue = Publisher::Queue;

    QueueMemory<Queue> memory;
    auto queue =
        Queue::initialize(memory.get());

    for (std::uint64_t sequence = 1;
         sequence <= 4;
         ++sequence) {
        ASSERT_TRUE(
            queue.try_push(
                pulsegrid::encode_delta(
                    make_update(
                        sequence,
                        sequence,
                        sequence
                    )
                )
            )
        );
    }

    Publisher publisher(queue);

    EXPECT_FALSE(
        publisher.publish(
            make_update(1, 5, 50)
        )
    );

    EXPECT_FALSE(
        publisher.publish(
            make_update(1, 6, 60)
        )
    );

    // Queue is still full.
    EXPECT_FALSE(
        publisher.try_flush()
    );

    EXPECT_TRUE(
        publisher.backpressured()
    );

    EXPECT_EQ(
        publisher.pending_cells(),
        1
    );

    EXPECT_EQ(
        publisher.pending_watermark(),
        6
    );
}

TEST(BackpressurePublisher, FlushesAtomicCoalescedTransaction) {
    using Publisher =
        pulsegrid::BackpressurePublisher<8>;

    using Queue = Publisher::Queue;

    QueueMemory<Queue> memory;
    auto queue =
        Queue::initialize(memory.get());

    // Consumer state after these direct deltas
    // will be sequence 5.
    for (std::uint64_t sequence = 1;
         sequence <= 5;
         ++sequence) {
        ASSERT_TRUE(
            queue.try_push(
                pulsegrid::encode_delta(
                    make_update(
                        100 + sequence,
                        sequence,
                        sequence
                    )
                )
            )
        );
    }

    Publisher publisher(queue);

    // Three free queue slots remain. Fill them
    // through the normal publisher path.
    EXPECT_TRUE(
        publisher.publish(
            make_update(106, 6, 6)
        )
    );

    EXPECT_TRUE(
        publisher.publish(
            make_update(107, 7, 7)
        )
    );

    EXPECT_TRUE(
        publisher.publish(
            make_update(108, 8, 8)
        )
    );

    // Queue now full. Start a coalesced interval.
    EXPECT_FALSE(
        publisher.publish(
            make_update(1, 9, 90)
        )
    );

    EXPECT_FALSE(
        publisher.publish(
            make_update(1, 10, 100)
        )
    );

    EXPECT_FALSE(
        publisher.publish(
            make_update(2, 11, 110)
        )
    );

    // Drain all eight ordinary deltas.
    pulsegrid::FrameDecoder decoder;
    pulsegrid::StateStore store;

    pulsegrid::TransportFrame frame{};

    while (queue.try_pop(frame)) {
        const auto result =
            decoder.consume(frame);

        ASSERT_TRUE(result.delta);
        store.apply(*result.delta);
    }

    ASSERT_TRUE(store.last_sequence());
    EXPECT_EQ(*store.last_sequence(), 8);

    // Coalescing retained two cells, therefore
    // BEGIN + 2 CELL + END = four frames.
    ASSERT_TRUE(
        publisher.try_flush()
    );

    EXPECT_FALSE(
        publisher.backpressured()
    );

    std::size_t frame_count = 0;

    while (queue.try_pop(frame)) {
        ++frame_count;

        auto result =
            decoder.consume(frame);

        if (result.delta) {
            store.apply(*result.delta);
        }

        if (result.coalesced) {
            store.apply_coalesced(
                *result.coalesced
            );
        }
    }

    EXPECT_EQ(frame_count, 4);

    ASSERT_TRUE(store.last_sequence());
    EXPECT_EQ(*store.last_sequence(), 11);

    const auto first =
        store.get(1, 1, 1);

    const auto second =
        store.get(1, 2, 1);

    ASSERT_TRUE(first);
    ASSERT_TRUE(second);

    EXPECT_EQ(
        std::get<std::uint64_t>(
            first->value
        ),
        100
    );

    EXPECT_EQ(
        std::get<std::uint64_t>(
            second->value
        ),
        110
    );
}

TEST(BackpressurePublisher, RejectsUnflushableDistinctCellGrowth) {
    using Publisher =
        pulsegrid::BackpressurePublisher<4>;

    using Queue = Publisher::Queue;

    QueueMemory<Queue> memory;
    auto queue =
        Queue::initialize(memory.get());

    for (std::uint64_t sequence = 1;
         sequence <= 4;
         ++sequence) {
        ASSERT_TRUE(
            queue.try_push(
                pulsegrid::encode_delta(
                    make_update(
                        100 + sequence,
                        sequence,
                        sequence
                    )
                )
            )
        );
    }

    Publisher publisher(queue);

    EXPECT_FALSE(
        publisher.publish(
            make_update(1, 5, 50)
        )
    );

    EXPECT_FALSE(
        publisher.publish(
            make_update(2, 6, 60)
        )
    );

    ASSERT_EQ(
        publisher.pending_cells(),
        2
    );

    ASSERT_EQ(
        publisher.pending_watermark(),
        6
    );

    EXPECT_THROW(
        (void)publisher.publish(
            make_update(3, 7, 70)
        ),
        pulsegrid::CoalescingOverflow
    );

    // Rejected publication did not mutate
    // pending state or sequence.
    EXPECT_EQ(
        publisher.pending_cells(),
        2
    );

    EXPECT_EQ(
        publisher.pending_watermark(),
        6
    );
}

TEST(BackpressurePublisher, AllowsExistingCellUpdateAtCapacity) {
    using Publisher =
        pulsegrid::BackpressurePublisher<4>;

    using Queue = Publisher::Queue;

    QueueMemory<Queue> memory;
    auto queue =
        Queue::initialize(memory.get());

    for (std::uint64_t sequence = 1;
         sequence <= 4;
         ++sequence) {
        ASSERT_TRUE(
            queue.try_push(
                pulsegrid::encode_delta(
                    make_update(
                        100 + sequence,
                        sequence,
                        sequence
                    )
                )
            )
        );
    }

    Publisher publisher(queue);

    EXPECT_FALSE(
        publisher.publish(
            make_update(1, 5, 50)
        )
    );

    EXPECT_FALSE(
        publisher.publish(
            make_update(2, 6, 60)
        )
    );

    // At the two-distinct-cell limit, but
    // replacing row 1 doesn't enlarge the
    // transaction.
    EXPECT_FALSE(
        publisher.publish(
            make_update(1, 7, 70)
        )
    );

    EXPECT_EQ(
        publisher.pending_cells(),
        2
    );

    EXPECT_EQ(
        publisher.pending_watermark(),
        7
    );
}
