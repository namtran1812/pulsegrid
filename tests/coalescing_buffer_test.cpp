#include <cstdint>
#include <unordered_map>

#include <gtest/gtest.h>

#include "pulsegrid/coalescing_buffer.hpp"

namespace {

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

} // namespace

TEST(CoalescingBuffer, KeepsLatestUpdatePerCell) {
    pulsegrid::CoalescingBuffer buffer;

    buffer.push(
        make_update(1, 1, 100, 10)
    );

    buffer.push(
        make_update(1, 1, 101, 20)
    );

    buffer.push(
        make_update(1, 1, 102, 30)
    );

    EXPECT_EQ(buffer.size(), 1);

    const auto batch = buffer.flush();

    ASSERT_EQ(batch.updates.size(), 1);
    EXPECT_EQ(batch.first_sequence, 100);
    EXPECT_EQ(batch.watermark, 102);
    EXPECT_EQ(batch.updates[0].sequence, 102);
    EXPECT_EQ(batch.updates[0].payload, 30);
}

TEST(CoalescingBuffer, KeepsDifferentCells) {
    pulsegrid::CoalescingBuffer buffer;

    buffer.push(
        make_update(1, 1, 50, 100)
    );

    buffer.push(
        make_update(2, 1, 51, 200)
    );

    buffer.push(
        make_update(1, 1, 52, 300)
    );

    const auto batch = buffer.flush();

    EXPECT_EQ(batch.watermark, 52);
    EXPECT_EQ(batch.updates.size(), 2);

    std::unordered_map<
        std::uint32_t,
        std::uint64_t
    > values;

    for (const auto& update :
         batch.updates) {
        values[update.row_id] =
            update.payload;
    }

    EXPECT_EQ(values[1], 300);
    EXPECT_EQ(values[2], 200);
}

TEST(CoalescingBuffer, FlushClearsPendingState) {
    pulsegrid::CoalescingBuffer buffer;

    buffer.push(
        make_update(1, 1, 1, 10)
    );

    EXPECT_FALSE(buffer.empty());

    const auto first = buffer.flush();

    EXPECT_EQ(first.watermark, 1);
    EXPECT_TRUE(buffer.empty());

    buffer.push(
        make_update(2, 1, 2, 20)
    );

    const auto second = buffer.flush();

    EXPECT_EQ(second.watermark, 2);
    ASSERT_EQ(second.updates.size(), 1);
    EXPECT_EQ(second.updates[0].payload, 20);
}

TEST(CoalescingBuffer, RejectsInputSequenceGap) {
    pulsegrid::CoalescingBuffer buffer;

    buffer.push(
        make_update(1, 1, 100, 10)
    );

    EXPECT_THROW(
        buffer.push(
            make_update(2, 1, 102, 20)
        ),
        pulsegrid::SequenceGap
    );

    ASSERT_TRUE(buffer.last_sequence());
    EXPECT_EQ(*buffer.last_sequence(), 100);
}

TEST(CoalescingBuffer, RejectsDuplicateSequence) {
    pulsegrid::CoalescingBuffer buffer;

    buffer.push(
        make_update(1, 1, 100, 10)
    );

    EXPECT_THROW(
        buffer.push(
            make_update(2, 1, 100, 20)
        ),
        pulsegrid::StaleSequence
    );

    EXPECT_EQ(buffer.size(), 1);
}

TEST(CoalescingBuffer, EmptyFlushHasZeroWatermark) {
    pulsegrid::CoalescingBuffer buffer;

    const auto batch = buffer.flush();

    EXPECT_EQ(batch.watermark, 0);
    EXPECT_TRUE(batch.updates.empty());
}

TEST(CoalescingBuffer, StateStoreAcceptsSequenceJumpViaBatch) {
    pulsegrid::StateStore store;

    store.apply(
        make_update(1, 1, 100, 10)
    );

    pulsegrid::CoalescingBuffer buffer;

    buffer.push(
        make_update(1, 1, 101, 11)
    );

    buffer.push(
        make_update(2, 1, 102, 20)
    );

    buffer.push(
        make_update(1, 1, 103, 12)
    );

    buffer.push(
        make_update(2, 1, 104, 21)
    );

    buffer.push(
        make_update(1, 1, 105, 13)
    );

    const auto batch = buffer.flush();

    EXPECT_NO_THROW(
        store.apply_coalesced(batch)
    );

    ASSERT_TRUE(store.last_sequence());
    EXPECT_EQ(*store.last_sequence(), 105);

    const auto first = store.get(1, 1, 1);
    const auto second = store.get(1, 2, 1);

    ASSERT_TRUE(first);
    ASSERT_TRUE(second);

    EXPECT_EQ(
        std::get<std::uint64_t>(
            first->value
        ),
        13
    );

    EXPECT_EQ(
        std::get<std::uint64_t>(
            second->value
        ),
        21
    );
}

TEST(CoalescingBuffer, OrdinaryDeltaStillRejectsJump) {
    pulsegrid::StateStore store;

    store.apply(
        make_update(1, 1, 100, 10)
    );

    EXPECT_THROW(
        store.apply(
            make_update(2, 1, 105, 20)
        ),
        pulsegrid::SequenceGap
    );

    ASSERT_TRUE(store.last_sequence());
    EXPECT_EQ(*store.last_sequence(), 100);
}

TEST(CoalescingBuffer, ContinuesNormallyAfterBatch) {
    pulsegrid::StateStore store;

    store.apply(
        make_update(1, 1, 100, 10)
    );

    pulsegrid::CoalescingBuffer buffer;

    for (std::uint64_t sequence = 101;
         sequence <= 110;
         ++sequence) {
        buffer.push(
            make_update(
                sequence % 3,
                1,
                sequence,
                sequence * 10ULL
            )
        );
    }

    store.apply_coalesced(
        buffer.flush()
    );

    ASSERT_TRUE(store.last_sequence());
    EXPECT_EQ(*store.last_sequence(), 110);

    EXPECT_NO_THROW(
        store.apply(
            make_update(
                10, 1, 111, 1110
            )
        )
    );

    EXPECT_EQ(
        *store.last_sequence(),
        111
    );
}

TEST(CoalescingBuffer, RejectsDuplicateCellsAtomically) {
    pulsegrid::StateStore store;

    store.apply(
        make_update(9, 1, 100, 999)
    );

    pulsegrid::CoalescedBatch batch{
        .first_sequence = 101,
        .watermark = 102,
        .updates = {
            make_update(1, 1, 101, 10),
            make_update(1, 1, 102, 20)
        }
    };

    EXPECT_THROW(
        store.apply_coalesced(batch),
        std::runtime_error
    );

    EXPECT_EQ(store.size(), 1);

    ASSERT_TRUE(store.last_sequence());
    EXPECT_EQ(*store.last_sequence(), 100);

    EXPECT_FALSE(
        store.get(1, 1, 1)
    );
}

TEST(CoalescingBuffer, RejectsUpdateBeyondWatermarkAtomically) {
    pulsegrid::StateStore store;

    store.apply(
        make_update(9, 1, 100, 999)
    );

    pulsegrid::CoalescedBatch batch{
        .first_sequence = 101,
        .watermark = 102,
        .updates = {
            make_update(1, 1, 101, 10),
            make_update(2, 1, 103, 20)
        }
    };

    EXPECT_THROW(
        store.apply_coalesced(batch),
        std::runtime_error
    );

    EXPECT_EQ(store.size(), 1);

    ASSERT_TRUE(store.last_sequence());
    EXPECT_EQ(*store.last_sequence(), 100);

    EXPECT_FALSE(store.get(1, 1, 1));
    EXPECT_FALSE(store.get(1, 2, 1));
}

TEST(CoalescingBuffer, RejectsBatchThatDoesNotBridgeCurrentSequence) {
    pulsegrid::StateStore store;

    store.apply(
        make_update(9, 1, 100, 999)
    );

    pulsegrid::CoalescedBatch batch{
        .first_sequence = 102,
        .watermark = 105,
        .updates = {
            make_update(1, 1, 103, 10),
            make_update(2, 1, 105, 20)
        }
    };

    EXPECT_THROW(
        store.apply_coalesced(batch),
        pulsegrid::SequenceGap
    );

    ASSERT_TRUE(store.last_sequence());
    EXPECT_EQ(*store.last_sequence(), 100);

    EXPECT_EQ(store.size(), 1);
    EXPECT_FALSE(store.get(1, 1, 1));
    EXPECT_FALSE(store.get(1, 2, 1));
}

TEST(
    CoalescingBuffer,
    EmptyFlushAfterPriorBatchHasZeroRange
) {
    pulsegrid::CoalescingBuffer buffer;

    buffer.push(
        make_update(1, 1, 1, 1)
    );

    const auto first = buffer.flush();

    EXPECT_EQ(first.first_sequence, 1);
    EXPECT_EQ(first.watermark, 1);
    EXPECT_EQ(first.updates.size(), 1);

    const auto second = buffer.flush();

    EXPECT_EQ(second.first_sequence, 0);
    EXPECT_EQ(second.watermark, 0);
    EXPECT_TRUE(second.updates.empty());

    // Flushing pending state does not forget the
    // global input sequence.
    ASSERT_TRUE(buffer.last_sequence());
    EXPECT_EQ(*buffer.last_sequence(), 1);

    // Therefore the next publication must still
    // continue the original sequence.
    buffer.push(
        make_update(1, 1, 2, 2)
    );

    const auto third = buffer.flush();

    EXPECT_EQ(third.first_sequence, 2);
    EXPECT_EQ(third.watermark, 2);
}

TEST(
    CoalescingBuffer,
    SnapshotUsesCanonicalCellOrder
) {
    pulsegrid::CoalescingBuffer buffer;

    buffer.push({
        .table_id = 2,
        .row_id = 1,
        .column_id = 1,
        .type = pulsegrid::ValueType::UInt64,
        .sequence = 1,
        .payload = 1
    });

    buffer.push({
        .table_id = 1,
        .row_id = 3,
        .column_id = 2,
        .type = pulsegrid::ValueType::UInt64,
        .sequence = 2,
        .payload = 2
    });

    buffer.push({
        .table_id = 1,
        .row_id = 3,
        .column_id = 1,
        .type = pulsegrid::ValueType::UInt64,
        .sequence = 3,
        .payload = 3
    });

    buffer.push({
        .table_id = 1,
        .row_id = 2,
        .column_id = 9,
        .type = pulsegrid::ValueType::UInt64,
        .sequence = 4,
        .payload = 4
    });

    const auto batch = buffer.snapshot();

    ASSERT_EQ(batch.updates.size(), 4);

    EXPECT_EQ(batch.updates[0].table_id, 1);
    EXPECT_EQ(batch.updates[0].row_id, 2);
    EXPECT_EQ(batch.updates[0].column_id, 9);

    EXPECT_EQ(batch.updates[1].table_id, 1);
    EXPECT_EQ(batch.updates[1].row_id, 3);
    EXPECT_EQ(batch.updates[1].column_id, 1);

    EXPECT_EQ(batch.updates[2].table_id, 1);
    EXPECT_EQ(batch.updates[2].row_id, 3);
    EXPECT_EQ(batch.updates[2].column_id, 2);

    EXPECT_EQ(batch.updates[3].table_id, 2);
    EXPECT_EQ(batch.updates[3].row_id, 1);
    EXPECT_EQ(batch.updates[3].column_id, 1);
}

TEST(CoalescingBuffer, RejectsZeroSequencePublication) {
    pulsegrid::CoalescingBuffer buffer;

    const pulsegrid::Update update{
        .table_id = 1,
        .row_id = 1,
        .column_id = 1,
        .type = pulsegrid::ValueType::UInt64,
        .sequence = 0,
        .payload = 42
    };

    EXPECT_THROW(
        buffer.push(update),
        pulsegrid::SequenceError
    );

    EXPECT_TRUE(buffer.empty());
    EXPECT_FALSE(buffer.last_sequence().has_value());
}

TEST(
    CoalescingBuffer,
    RejectsBatchWithoutWatermarkUpdateAtomically
) {
    pulsegrid::StateStore store;

    store.apply(
        pulsegrid::Update{
            .table_id = 1,
            .row_id = 1,
            .column_id = 1,
            .type = pulsegrid::ValueType::UInt64,
            .sequence = 100,
            .payload = 100
        }
    );

    const pulsegrid::CoalescedBatch batch{
        .first_sequence = 101,
        .watermark = 103,
        .updates = {
            pulsegrid::Update{
                .table_id = 1,
                .row_id = 2,
                .column_id = 1,
                .type = pulsegrid::ValueType::UInt64,
                .sequence = 101,
                .payload = 101
            },
            pulsegrid::Update{
                .table_id = 1,
                .row_id = 3,
                .column_id = 1,
                .type = pulsegrid::ValueType::UInt64,
                .sequence = 102,
                .payload = 102
            }
        }
    };

    EXPECT_THROW(
        store.apply_coalesced(batch),
        std::runtime_error
    );

    ASSERT_TRUE(store.last_sequence().has_value());
    EXPECT_EQ(*store.last_sequence(), 100U);

    EXPECT_FALSE(store.get(1, 2, 1).has_value());
    EXPECT_FALSE(store.get(1, 3, 1).has_value());
}
