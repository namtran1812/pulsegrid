#include <gtest/gtest.h>

#include "pulsegrid/transport_frame.hpp"

TEST(TransportFrame, IsTriviallyCopyable) {
    EXPECT_TRUE(
        std::is_trivially_copyable_v<
            pulsegrid::TransportFrame
        >
    );
}

TEST(TransportFrame, RepresentsOrdinaryDelta) {
    pulsegrid::TransportFrame frame{
        .kind =
            pulsegrid::FrameKind::Delta,
        .first_sequence = 42,
        .watermark = 42,
        .cell_count = 0,
        .update = {
            .table_id = 1,
            .row_id = 2,
            .column_id = 3,
            .type =
                pulsegrid::ValueType::UInt64,
            .sequence = 42,
            .payload = 999
        }
    };

    EXPECT_EQ(
        frame.kind,
        pulsegrid::FrameKind::Delta
    );

    EXPECT_EQ(frame.first_sequence, 42);
    EXPECT_EQ(frame.watermark, 42);
    EXPECT_EQ(frame.update.sequence, 42);
}

#include <cstddef>
#include <memory>

#include "pulsegrid/shm_queue.hpp"

TEST(TransportFrame, WorksWithGenericShmQueue) {
    using Queue =
        pulsegrid::ShmQueue<
            8,
            pulsegrid::TransportFrame
        >;

    void* memory =
        ::operator new(
            Queue::mapped_size(),
            std::align_val_t{
                pulsegrid::kShmCacheLine
            }
        );

    auto queue =
        Queue::initialize(memory);

    const pulsegrid::TransportFrame input{
        .kind =
            pulsegrid::FrameKind::Delta,
        .first_sequence = 42,
        .watermark = 42,
        .cell_count = 0,
        .update = {
            .table_id = 1,
            .row_id = 2,
            .column_id = 3,
            .type =
                pulsegrid::ValueType::UInt64,
            .sequence = 42,
            .payload = 999
        }
    };

    ASSERT_TRUE(
        queue.try_push(input)
    );

    pulsegrid::TransportFrame output{};

    ASSERT_TRUE(
        queue.try_pop(output)
    );

    EXPECT_EQ(
        output.kind,
        pulsegrid::FrameKind::Delta
    );

    EXPECT_EQ(
        output.first_sequence,
        42
    );

    EXPECT_EQ(
        output.update.payload,
        999
    );

    ::operator delete(
        memory,
        std::align_val_t{
            pulsegrid::kShmCacheLine
        }
    );
}

TEST(
    TransportFrame,
    RejectsSharedMemoryPayloadSizeMismatch
) {
    constexpr std::size_t kCapacity = 8;

    using UpdateQueue =
        pulsegrid::ShmQueue<
            kCapacity,
            pulsegrid::Update
        >;

    using FrameQueue =
        pulsegrid::ShmQueue<
            kCapacity,
            pulsegrid::TransportFrame
        >;

    static_assert(
        sizeof(pulsegrid::Update) !=
        sizeof(pulsegrid::TransportFrame)
    );

    void* memory =
        ::operator new(
            FrameQueue::mapped_size(),
            std::align_val_t{
                pulsegrid::kShmCacheLine
            }
        );

    (void)UpdateQueue::initialize(memory);

    EXPECT_THROW(
        (void)FrameQueue::attach(memory),
        std::runtime_error
    );

    ::operator delete(
        memory,
        std::align_val_t{
            pulsegrid::kShmCacheLine
        }
    );
}

TEST(
    TransportFrame,
    RejectsCorruptedPayloadAlignmentMetadata
) {
    constexpr std::size_t kCapacity = 8;

    using Queue =
        pulsegrid::ShmQueue<
            kCapacity,
            pulsegrid::TransportFrame
        >;

    void* memory =
        ::operator new(
            Queue::mapped_size(),
            std::align_val_t{
                pulsegrid::kShmCacheLine
            }
        );

    (void)Queue::initialize(memory);

    auto* layout =
        static_cast<Queue::Layout*>(memory);

    layout->header.entry_alignment += 1;

    EXPECT_THROW(
        (void)Queue::attach(memory),
        std::runtime_error
    );

    ::operator delete(
        memory,
        std::align_val_t{
            pulsegrid::kShmCacheLine
        }
    );
}
