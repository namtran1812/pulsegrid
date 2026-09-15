#include <cstdint>

#include <gtest/gtest.h>
#include <optional>

#include "pulsegrid/coalescing_buffer.hpp"
#include <limits>

#include "pulsegrid/frame_codec.hpp"
#include "pulsegrid/state_store.hpp"

namespace {

pulsegrid::Update update(
    std::uint32_t row,
    std::uint64_t sequence,
    std::uint64_t value
) {
    return {
        .table_id = 1,
        .row_id = row,
        .column_id = 1,
        .type =
            pulsegrid::ValueType::UInt64,
        .sequence = sequence,
        .payload = value
    };
}

} // namespace

TEST(FrameCodec, DeltaRoundTrips) {
    const auto original =
        update(1, 42, 999);

    const auto frame =
        pulsegrid::encode_delta(
            original
        );

    pulsegrid::FrameDecoder decoder;

    const auto result =
        decoder.consume(frame);

    ASSERT_TRUE(result.delta);
    EXPECT_FALSE(result.coalesced);

    EXPECT_EQ(
        result.delta->update.sequence,
        42
    );

    EXPECT_EQ(
        result.delta->update.payload,
        999
    );
}

TEST(FrameCodec, CoalescedBatchRoundTrips) {
    const pulsegrid::CoalescedBatch batch{
        .first_sequence = 101,
        .watermark = 105,
        .updates = {
            update(1, 103, 30),
            update(2, 105, 50)
        }
    };

    const auto frames =
        pulsegrid::encode_coalesced(
            batch
        );

    ASSERT_EQ(frames.size(), 4);

    pulsegrid::FrameDecoder decoder;

    std::optional<
        pulsegrid::CoalescedBatch
    > decoded;

    for (const auto& frame : frames) {
        const auto result =
            decoder.consume(frame);

        if (result.coalesced) {
            decoded =
                std::move(result.coalesced->batch);
        }
    }

    ASSERT_TRUE(decoded);

    EXPECT_EQ(
        decoded->first_sequence,
        101
    );

    EXPECT_EQ(
        decoded->watermark,
        105
    );

    ASSERT_EQ(
        decoded->updates.size(),
        2
    );
}

TEST(FrameCodec, IncompleteBatchProducesNoState) {
    const pulsegrid::CoalescedBatch batch{
        .first_sequence = 101,
        .watermark = 105,
        .updates = {
            update(1, 103, 30),
            update(2, 105, 50)
        }
    };

    const auto frames =
        pulsegrid::encode_coalesced(
            batch
        );

    pulsegrid::FrameDecoder decoder;

    // Consume BEGIN and one CELL only.
    const auto first =
        decoder.consume(frames[0]);

    const auto second =
        decoder.consume(frames[1]);

    EXPECT_FALSE(first.delta);
    EXPECT_FALSE(first.coalesced);
    EXPECT_FALSE(second.delta);
    EXPECT_FALSE(second.coalesced);

    EXPECT_TRUE(decoder.in_batch());
}

TEST(FrameCodec, RejectsPrematureEnd) {
    const pulsegrid::CoalescedBatch batch{
        .first_sequence = 101,
        .watermark = 105,
        .updates = {
            update(1, 103, 30),
            update(2, 105, 50)
        }
    };

    const auto frames =
        pulsegrid::encode_coalesced(
            batch
        );

    pulsegrid::FrameDecoder decoder;

    (void)decoder.consume(frames[0]);
    (void)decoder.consume(frames[1]);

    EXPECT_THROW(
        (void)decoder.consume(
            frames.back()
        ),
        std::runtime_error
    );
}

TEST(FrameCodec, DecodedBatchAppliesAtomically) {
    pulsegrid::StateStore store;

    store.apply(
        update(9, 100, 999)
    );

    const pulsegrid::CoalescedBatch batch{
        .first_sequence = 101,
        .watermark = 105,
        .updates = {
            update(1, 103, 30),
            update(2, 105, 50)
        }
    };

    const auto frames =
        pulsegrid::encode_coalesced(
            batch
        );

    pulsegrid::FrameDecoder decoder;

    for (const auto& frame : frames) {
        auto result =
            decoder.consume(frame);

        if (result.coalesced) {
            store.apply_coalesced(
                result.coalesced->epoch,
                result.coalesced->batch
            );
        }
    }

    ASSERT_TRUE(store.last_sequence());

    EXPECT_EQ(
        *store.last_sequence(),
        105
    );

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
        30
    );

    EXPECT_EQ(
        std::get<std::uint64_t>(
            second->value
        ),
        50
    );
}

TEST(
    FrameCodec,
    PreservesStateAcrossLegacy32BitSequenceBoundary
) {
    constexpr std::uint64_t kBeforeBoundary =
        static_cast<std::uint64_t>(
            std::numeric_limits<std::uint32_t>::max()
        ) - 1;

    constexpr std::uint64_t kBoundary =
        static_cast<std::uint64_t>(
            std::numeric_limits<std::uint32_t>::max()
        );

    constexpr std::uint64_t kAfterBoundary =
        kBoundary + 1;

    constexpr std::uint64_t kAfterBoundary2 =
        kBoundary + 2;

    constexpr std::uint64_t kAfterBoundary3 =
        kBoundary + 3;

    pulsegrid::StateStore store;

    // Establish state immediately before the old
    // 32-bit sequence limit.
    const pulsegrid::Update initial{
        .table_id = 1,
        .row_id = 10,
        .column_id = 1,
        .type = pulsegrid::ValueType::UInt64,
        .sequence = kBeforeBoundary,
        .payload = 100
    };

    store.apply(initial);

    ASSERT_TRUE(store.last_sequence().has_value());
    EXPECT_EQ(
        *store.last_sequence(),
        kBeforeBoundary
    );

    // Coalesce publications spanning UINT32_MAX.
    pulsegrid::CoalescingBuffer buffer;

    buffer.push(
        pulsegrid::Update{
            .table_id = 1,
            .row_id = 10,
            .column_id = 1,
            .type = pulsegrid::ValueType::UInt64,
            .sequence = kBoundary,
            .payload = 200
        }
    );

    buffer.push(
        pulsegrid::Update{
            .table_id = 1,
            .row_id = 20,
            .column_id = 1,
            .type = pulsegrid::ValueType::UInt64,
            .sequence = kAfterBoundary,
            .payload = 300
        }
    );

    buffer.push(
        pulsegrid::Update{
            .table_id = 1,
            .row_id = 10,
            .column_id = 1,
            .type = pulsegrid::ValueType::UInt64,
            .sequence = kAfterBoundary2,
            .payload = 400
        }
    );

    const auto batch = buffer.flush();

    EXPECT_EQ(
        batch.first_sequence,
        kBoundary
    );

    EXPECT_EQ(
        batch.watermark,
        kAfterBoundary2
    );

    ASSERT_EQ(batch.updates.size(), 2U);

    // Encode the coalesced transaction into transport
    // frames and reconstruct it through the decoder.
    const auto frames =
        pulsegrid::encode_coalesced(batch);

    pulsegrid::FrameDecoder decoder;

    std::optional<pulsegrid::CoalescedBatch>
        decoded_batch;

    for (const auto& frame : frames) {
        auto result = decoder.consume(frame);

        EXPECT_FALSE(result.delta.has_value());

        if (result.coalesced) {
            ASSERT_FALSE(
                decoded_batch.has_value()
            );

            decoded_batch =
                std::move(result.coalesced->batch);
        }
    }

    ASSERT_TRUE(decoded_batch.has_value());
    EXPECT_FALSE(decoder.in_batch());

    EXPECT_EQ(
        decoded_batch->first_sequence,
        kBoundary
    );

    EXPECT_EQ(
        decoded_batch->watermark,
        kAfterBoundary2
    );

    // StateStore must accept the represented sequence
    // interval across the old uint32_t boundary.
    store.apply_coalesced(*decoded_batch);

    ASSERT_TRUE(store.last_sequence().has_value());
    EXPECT_EQ(
        *store.last_sequence(),
        kAfterBoundary2
    );

    // Snapshot and recovery must preserve the full
    // 64-bit watermark.
    const auto snapshot = store.snapshot();

    EXPECT_EQ(
        snapshot.sequence,
        kAfterBoundary2
    );

    auto restored =
        pulsegrid::StateStore::restore(snapshot);

    ASSERT_TRUE(
        restored.last_sequence().has_value()
    );

    EXPECT_EQ(
        *restored.last_sequence(),
        kAfterBoundary2
    );

    // The next ordinary publication must continue at
    // UINT32_MAX + 3 rather than wrapping to zero.
    const pulsegrid::Update next{
        .table_id = 1,
        .row_id = 30,
        .column_id = 1,
        .type = pulsegrid::ValueType::UInt64,
        .sequence = kAfterBoundary3,
        .payload = 500
    };

    const auto delta_frame =
        pulsegrid::encode_delta(next);

    auto result =
        decoder.consume(delta_frame);

    ASSERT_TRUE(result.delta.has_value());
    EXPECT_FALSE(result.coalesced.has_value());

    EXPECT_EQ(
        result.delta->update.sequence,
        kAfterBoundary3
    );

    restored.apply(
        result.delta->epoch,
        result.delta->update
    );

    ASSERT_TRUE(
        restored.last_sequence().has_value()
    );

    EXPECT_EQ(
        *restored.last_sequence(),
        kAfterBoundary3
    );
}

TEST(FrameCodecEpoch, DeltaCarriesBoundEpoch) {
    constexpr std::uint64_t kEpoch = 42;

    pulsegrid::FrameDecoder decoder(kEpoch);

    const auto frame =
        pulsegrid::encode_delta(
            update(1, 1, 100)
        );

    const auto result =
        decoder.consume(frame);

    ASSERT_TRUE(result.delta);
    EXPECT_FALSE(result.coalesced);

    EXPECT_EQ(result.delta->epoch, kEpoch);
    EXPECT_EQ(
        result.delta->update.sequence,
        1U
    );
    EXPECT_EQ(
        result.delta->update.payload,
        100U
    );
}

TEST(FrameCodecEpoch, CoalescedBatchCarriesBoundEpoch) {
    constexpr std::uint64_t kEpoch = 99;

    const pulsegrid::CoalescedBatch batch{
        .first_sequence = 10,
        .watermark = 12,
        .updates = {
            update(1, 11, 110),
            update(2, 12, 120)
        }
    };

    const auto frames =
        pulsegrid::encode_coalesced(batch);

    pulsegrid::FrameDecoder decoder(kEpoch);

    std::optional<
        pulsegrid::DecodedCoalesced
    > decoded;

    for (const auto& frame : frames) {
        auto result =
            decoder.consume(frame);

        if (result.coalesced) {
            decoded =
                std::move(result.coalesced);
        }
    }

    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->epoch, kEpoch);
    EXPECT_EQ(
        decoded->batch.first_sequence,
        10U
    );
    EXPECT_EQ(
        decoded->batch.watermark,
        12U
    );
    EXPECT_EQ(
        decoded->batch.updates.size(),
        2U
    );
}

TEST(FrameCodecEpoch, RejectsZeroEpoch) {
    EXPECT_THROW(
        (void)pulsegrid::FrameDecoder(0),
        std::runtime_error
    );
}

TEST(FrameCodecEpoch, RebindsBetweenTransactions) {
    pulsegrid::FrameDecoder decoder(10);

    {
        const auto result =
            decoder.consume(
                pulsegrid::encode_delta(
                    update(1, 1, 100)
                )
            );

        ASSERT_TRUE(result.delta);
        EXPECT_EQ(result.delta->epoch, 10U);
    }

    EXPECT_NO_THROW(
        decoder.bind_epoch(20)
    );

    {
        const auto result =
            decoder.consume(
                pulsegrid::encode_delta(
                    update(2, 1, 200)
                )
            );

        ASSERT_TRUE(result.delta);
        EXPECT_EQ(result.delta->epoch, 20U);
    }
}

TEST(FrameCodecEpoch, RejectsRebindDuringBatch) {
    const pulsegrid::CoalescedBatch batch{
        .first_sequence = 1,
        .watermark = 2,
        .updates = {
            update(1, 1, 100),
            update(2, 2, 200)
        }
    };

    const auto frames =
        pulsegrid::encode_coalesced(batch);

    pulsegrid::FrameDecoder decoder(10);

    (void)decoder.consume(frames.front());

    ASSERT_TRUE(decoder.in_batch());

    EXPECT_THROW(
        decoder.bind_epoch(20),
        std::runtime_error
    );

    EXPECT_EQ(decoder.epoch(), 10U);
    EXPECT_TRUE(decoder.in_batch());
}

TEST(FrameCodecEpoch, StateStoreRejectsOldEpochAfterRecovery) {
    pulsegrid::FrameDecoder old_decoder(10);

    auto old_result =
        old_decoder.consume(
            pulsegrid::encode_delta(
                update(1, 1, 100)
            )
        );

    ASSERT_TRUE(old_result.delta);

    pulsegrid::StateStore old_store;
    old_store.apply(
        old_result.delta->epoch,
        old_result.delta->update
    );

    pulsegrid::StateStore replacement;
    replacement.apply(
        20,
        update(9, 1, 900)
    );

    auto recovered =
        pulsegrid::StateStore::restore(
            replacement.snapshot()
        );

    ASSERT_TRUE(recovered.epoch());
    EXPECT_EQ(*recovered.epoch(), 20U);

    auto delayed_old =
        old_decoder.consume(
            pulsegrid::encode_delta(
                update(2, 2, 200)
            )
        );

    ASSERT_TRUE(delayed_old.delta);

    EXPECT_THROW(
        recovered.apply(
            delayed_old.delta->epoch,
            delayed_old.delta->update
        ),
        pulsegrid::EpochMismatch
    );

    ASSERT_TRUE(recovered.epoch());
    EXPECT_EQ(*recovered.epoch(), 20U);

    ASSERT_TRUE(recovered.last_sequence());
    EXPECT_EQ(
        *recovered.last_sequence(),
        1U
    );

    EXPECT_FALSE(
        recovered.get(1, 2, 1)
    );
}
