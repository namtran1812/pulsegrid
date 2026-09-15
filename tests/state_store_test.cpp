#include <bit>
#include <cstdint>

#include <gtest/gtest.h>

#include "pulsegrid/state_store.hpp"

namespace {

pulsegrid::Update make_uint_update(
    std::uint32_t table,
    std::uint32_t row,
    std::uint16_t column,
    std::uint32_t sequence,
    std::uint64_t value
) {
    return {
        .table_id = table,
        .row_id = row,
        .column_id = column,
        .type = pulsegrid::ValueType::UInt64,
        .sequence = sequence,
        .timestamp_ns = sequence,
        .payload = value
    };
}

} // namespace

TEST(StateStore, AppliesAndReadsUpdate) {
    pulsegrid::StateStore store;

    store.apply(
        make_uint_update(
            1,
            42,
            3,
            1,
            12345
        )
    );

    const auto cell =
        store.get(1, 42, 3);

    ASSERT_TRUE(cell.has_value());

    EXPECT_EQ(
        std::get<std::uint64_t>(
            cell->value
        ),
        12345
    );

    EXPECT_EQ(cell->sequence, 1);
}

TEST(StateStore, LatestUpdateWins) {
    pulsegrid::StateStore store;

    store.apply(
        make_uint_update(
            1, 42, 3, 1, 100
        )
    );

    store.apply(
        make_uint_update(
            1, 42, 3, 2, 200
        )
    );

    const auto cell =
        store.get(1, 42, 3);

    ASSERT_TRUE(cell.has_value());

    EXPECT_EQ(
        std::get<std::uint64_t>(
            cell->value
        ),
        200
    );

    EXPECT_EQ(cell->sequence, 2);
}

TEST(StateStore, RejectsStaleUpdate) {
    pulsegrid::StateStore store;

    store.apply(
        make_uint_update(
            1, 42, 3, 10, 100
        )
    );

    EXPECT_THROW(
        store.apply(
            make_uint_update(
                1, 42, 3, 9, 200
            )
        ),
        std::runtime_error
    );
}

TEST(StateStore, SupportsAllValueTypes) {
    pulsegrid::StateStore store;

    store.apply({
        .table_id = 1,
        .row_id = 1,
        .column_id = 1,
        .type = pulsegrid::ValueType::Int64,
        .sequence = 1,
        .timestamp_ns = 1,
        .payload =
            std::bit_cast<std::uint64_t>(
                std::int64_t{-42}
            )
    });

    store.apply({
        .table_id = 1,
        .row_id = 1,
        .column_id = 2,
        .type = pulsegrid::ValueType::Double,
        .sequence = 2,
        .timestamp_ns = 2,
        .payload =
            std::bit_cast<std::uint64_t>(
                3.14159
            )
    });

    const auto signed_cell =
        store.get(1, 1, 1);

    const auto double_cell =
        store.get(1, 1, 2);

    ASSERT_TRUE(signed_cell);
    ASSERT_TRUE(double_cell);

    EXPECT_EQ(
        std::get<std::int64_t>(
            signed_cell->value
        ),
        -42
    );

    EXPECT_DOUBLE_EQ(
        std::get<double>(
            double_cell->value
        ),
        3.14159
    );
}

TEST(StateStore, SnapshotContainsLatestState) {
    pulsegrid::StateStore store;

    store.apply(
        make_uint_update(
            1, 1, 1, 1, 10
        )
    );

    store.apply(
        make_uint_update(
            1, 2, 1, 2, 20
        )
    );

    store.apply(
        make_uint_update(
            1, 1, 1, 3, 30
        )
    );

    const auto snapshot =
        store.snapshot();

    EXPECT_EQ(snapshot.sequence, 3);
    EXPECT_EQ(snapshot.updates.size(), 2);
    EXPECT_EQ(store.size(), 2);
}


TEST(StateStore, TracksGlobalSequence) {
    pulsegrid::StateStore store;

    store.apply(
        make_uint_update(
            1, 1, 1, 100, 10
        )
    );

    store.apply(
        make_uint_update(
            1, 2, 1, 101, 20
        )
    );

    ASSERT_TRUE(
        store.last_sequence().has_value()
    );

    EXPECT_EQ(
        *store.last_sequence(),
        101
    );
}

TEST(StateStore, RejectsGlobalSequenceGap) {
    pulsegrid::StateStore store;

    store.apply(
        make_uint_update(
            1, 1, 1, 100, 10
        )
    );

    EXPECT_THROW(
        store.apply(
            make_uint_update(
                1, 2, 1, 102, 20
            )
        ),
        pulsegrid::SequenceGap
    );

    ASSERT_TRUE(store.last_sequence());
    EXPECT_EQ(*store.last_sequence(), 100);

    EXPECT_FALSE(
        store.get(1, 2, 1).has_value()
    );
}

TEST(StateStore, RejectsDuplicateGlobalSequence) {
    pulsegrid::StateStore store;

    store.apply(
        make_uint_update(
            1, 1, 1, 100, 10
        )
    );

    EXPECT_THROW(
        store.apply(
            make_uint_update(
                1, 2, 1, 100, 20
            )
        ),
        pulsegrid::StaleSequence
    );

    EXPECT_EQ(store.size(), 1);
}

TEST(StateStore, SequenceSpansDifferentCells) {
    pulsegrid::StateStore store;

    store.apply(
        make_uint_update(
            10, 100, 1, 50, 111
        )
    );

    store.apply(
        make_uint_update(
            20, 200, 2, 51, 222
        )
    );

    store.apply(
        make_uint_update(
            30, 300, 3, 52, 333
        )
    );

    EXPECT_EQ(store.size(), 3);

    ASSERT_TRUE(store.last_sequence());

    EXPECT_EQ(
        *store.last_sequence(),
        52
    );
}

TEST(StateStore, RestoresSnapshot) {
    pulsegrid::StateStore original;

    original.apply(
        make_uint_update(
            1, 1, 1, 100, 10
        )
    );

    original.apply(
        make_uint_update(
            1, 2, 1, 101, 20
        )
    );

    original.apply(
        make_uint_update(
            1, 1, 1, 102, 30
        )
    );

    const auto snapshot =
        original.snapshot();

    auto restored =
        pulsegrid::StateStore::restore(
            snapshot
        );

    EXPECT_EQ(restored.size(), 2);

    ASSERT_TRUE(restored.last_sequence());

    EXPECT_EQ(
        *restored.last_sequence(),
        102
    );

    const auto first =
        restored.get(1, 1, 1);

    const auto second =
        restored.get(1, 2, 1);

    ASSERT_TRUE(first);
    ASSERT_TRUE(second);

    EXPECT_EQ(
        std::get<std::uint64_t>(
            first->value
        ),
        30
    );

    EXPECT_EQ(
        std::get<std::uint64_t>(
            second->value
        ),
        20
    );
}

TEST(StateStore, ContinuesSequenceAfterRestore) {
    pulsegrid::StateStore original;

    original.apply(
        make_uint_update(
            1, 1, 1, 500, 10
        )
    );

    const auto snapshot =
        original.snapshot();

    auto restored =
        pulsegrid::StateStore::restore(
            snapshot
        );

    EXPECT_NO_THROW(
        restored.apply(
            make_uint_update(
                1, 2, 1, 501, 20
            )
        )
    );

    EXPECT_THROW(
        restored.apply(
            make_uint_update(
                1, 3, 1, 503, 30
            )
        ),
        pulsegrid::SequenceGap
    );
}

TEST(StateStore, SnapshotReplayMatchesUninterruptedState) {
    constexpr std::uint32_t
        kSnapshotSequence = 10'000;

    constexpr std::uint32_t
        kFinalSequence = 20'000;

    constexpr std::uint32_t
        kRows = 257;

    constexpr std::uint16_t
        kColumns = 8;

    pulsegrid::StateStore uninterrupted;
    pulsegrid::StateStore before_snapshot;

    auto make_stream_update =
        [](std::uint32_t sequence) {
            const auto row =
                sequence % kRows;

            const auto column =
                static_cast<std::uint16_t>(
                    sequence % kColumns
                );

            return make_uint_update(
                7,
                row,
                column,
                sequence,
                static_cast<std::uint64_t>(
                    sequence
                ) * 17
            );
        };

    for (std::uint32_t sequence = 1;
         sequence <= kSnapshotSequence;
         ++sequence) {

        const auto update =
            make_stream_update(sequence);

        uninterrupted.apply(update);
        before_snapshot.apply(update);
    }

    const auto snapshot =
        before_snapshot.snapshot();

    auto recovered =
        pulsegrid::StateStore::restore(
            snapshot
        );

    for (
        std::uint32_t sequence =
            kSnapshotSequence + 1;
        sequence <= kFinalSequence;
        ++sequence
    ) {
        const auto update =
            make_stream_update(sequence);

        uninterrupted.apply(update);
        recovered.apply(update);
    }

    ASSERT_TRUE(
        uninterrupted.last_sequence()
    );

    ASSERT_TRUE(
        recovered.last_sequence()
    );

    EXPECT_EQ(
        *uninterrupted.last_sequence(),
        kFinalSequence
    );

    EXPECT_EQ(
        *recovered.last_sequence(),
        kFinalSequence
    );

    EXPECT_EQ(
        uninterrupted.size(),
        recovered.size()
    );

    for (std::uint32_t row = 0;
         row < kRows;
         ++row) {

        for (std::uint16_t column = 0;
             column < kColumns;
             ++column) {

            const auto expected =
                uninterrupted.get(
                    7,
                    row,
                    column
                );

            const auto actual =
                recovered.get(
                    7,
                    row,
                    column
                );

            ASSERT_EQ(
                expected.has_value(),
                actual.has_value()
            );

            if (!expected) {
                continue;
            }

            EXPECT_EQ(
                expected->value,
                actual->value
            );

            EXPECT_EQ(
                expected->sequence,
                actual->sequence
            );

            EXPECT_EQ(
                expected->timestamp_ns,
                actual->timestamp_ns
            );
        }
    }
}

TEST(StateStore, DetectsMissingDeltaDuringReplay) {
    pulsegrid::StateStore original;

    for (std::uint32_t sequence = 1;
         sequence <= 1'000;
         ++sequence) {

        original.apply(
            make_uint_update(
                1,
                sequence % 64,
                static_cast<std::uint16_t>(
                    sequence % 4
                ),
                sequence,
                sequence * 10ULL
            )
        );
    }

    const auto snapshot =
        original.snapshot();

    auto recovered =
        pulsegrid::StateStore::restore(
            snapshot
        );

    // 1001 is the first valid post-snapshot delta.
    recovered.apply(
        make_uint_update(
            1,
            1,
            1,
            1'001,
            10'010
        )
    );

    // Simulate loss of sequence 1002.
    EXPECT_THROW(
        recovered.apply(
            make_uint_update(
                1,
                2,
                1,
                1'003,
                10'030
            )
        ),
        pulsegrid::SequenceGap
    );

    // Rejected update must not advance stream state.
    ASSERT_TRUE(
        recovered.last_sequence()
    );

    EXPECT_EQ(
        *recovered.last_sequence(),
        1'001
    );

    // Once the missing delta arrives, replay can
    // continue normally.
    EXPECT_NO_THROW(
        recovered.apply(
            make_uint_update(
                1,
                2,
                1,
                1'002,
                10'020
            )
        )
    );

    EXPECT_NO_THROW(
        recovered.apply(
            make_uint_update(
                1,
                3,
                1,
                1'003,
                10'030
            )
        )
    );

    ASSERT_TRUE(
        recovered.last_sequence()
    );

    EXPECT_EQ(
        *recovered.last_sequence(),
        1'003
    );
}

TEST(StateStore, RejectsSnapshotCellBeyondWatermark) {
    pulsegrid::Snapshot snapshot{
        .sequence = 100,
        .updates = {
            make_uint_update(
                1, 1, 1, 101, 10
            )
        }
    };

    EXPECT_THROW(
        pulsegrid::StateStore::restore(
            snapshot
        ),
        std::runtime_error
    );
}

TEST(StateStore, RejectsDuplicateCellsInSnapshot) {
    pulsegrid::Snapshot snapshot{
        .sequence = 100,
        .updates = {
            make_uint_update(
                1, 1, 1, 99, 10
            ),
            make_uint_update(
                1, 1, 1, 100, 20
            )
        }
    };

    EXPECT_THROW(
        pulsegrid::StateStore::restore(
            snapshot
        ),
        std::runtime_error
    );
}

TEST(StateStore, RejectsNonEmptyZeroSequenceSnapshot) {
    pulsegrid::Snapshot snapshot{
        .sequence = 0,
        .updates = {
            make_uint_update(
                1, 1, 1, 0, 10
            )
        }
    };

    EXPECT_THROW(
        pulsegrid::StateStore::restore(
            snapshot
        ),
        std::runtime_error
    );
}

TEST(StateStore, RestoresEmptySnapshot) {
    const pulsegrid::Snapshot snapshot{
        .sequence = 0,
        .updates = {}
    };

    auto restored =
        pulsegrid::StateStore::restore(
            snapshot
        );

    EXPECT_EQ(restored.size(), 0);
    EXPECT_FALSE(restored.last_sequence());
}
