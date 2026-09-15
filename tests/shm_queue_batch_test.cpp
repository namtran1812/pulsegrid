#include <array>
#include <cstddef>
#include <cstdint>

#include <gtest/gtest.h>

#include "pulsegrid/shm_queue.hpp"

TEST(ShmQueueBatch, PreservesFIFOOrder) {
    constexpr std::size_t kCapacity = 16;

    using Queue =
        pulsegrid::ShmQueue<kCapacity>;

    alignas(Queue::Layout)
    std::array<
        std::byte,
        Queue::mapped_size()
    > memory{};

    auto queue =
        Queue::initialize(memory.data());

    std::array<pulsegrid::Update, 8> input{};

    for (std::size_t i = 0;
         i < input.size();
         ++i) {

        input[i].sequence =
            static_cast<std::uint32_t>(i);

        input[i].payload = i;
    }

    ASSERT_TRUE(
        queue.try_push_batch(
            input.data(),
            input.size()
        )
    );

    std::array<pulsegrid::Update, 8> output{};

    const auto count =
        queue.try_pop_batch(
            output.data(),
            output.size()
        );

    ASSERT_EQ(count, input.size());

    for (std::size_t i = 0;
         i < output.size();
         ++i) {

        EXPECT_EQ(output[i].sequence, i);
        EXPECT_EQ(output[i].payload, i);
    }
}

TEST(ShmQueueBatch, RejectsBatchWithoutEnoughSpace) {
    constexpr std::size_t kCapacity = 8;

    using Queue =
        pulsegrid::ShmQueue<kCapacity>;

    alignas(Queue::Layout)
    std::array<
        std::byte,
        Queue::mapped_size()
    > memory{};

    auto queue =
        Queue::initialize(memory.data());

    std::array<pulsegrid::Update, 6> first{};
    std::array<pulsegrid::Update, 4> second{};

    ASSERT_TRUE(
        queue.try_push_batch(
            first.data(),
            first.size()
        )
    );

    EXPECT_FALSE(
        queue.try_push_batch(
            second.data(),
            second.size()
        )
    );
}

TEST(ShmQueueBatch, HandlesRingWraparound) {
    constexpr std::size_t kCapacity = 8;

    using Queue =
        pulsegrid::ShmQueue<kCapacity>;

    alignas(Queue::Layout)
    std::array<
        std::byte,
        Queue::mapped_size()
    > memory{};

    auto queue =
        Queue::initialize(memory.data());

    std::array<pulsegrid::Update, 6> first{};

    for (std::size_t i = 0;
         i < first.size();
         ++i) {

        first[i].sequence =
            static_cast<std::uint32_t>(i);
    }

    ASSERT_TRUE(
        queue.try_push_batch(
            first.data(),
            first.size()
        )
    );

    std::array<pulsegrid::Update, 4> discarded{};

    ASSERT_EQ(
        queue.try_pop_batch(
            discarded.data(),
            discarded.size()
        ),
        discarded.size()
    );

    std::array<pulsegrid::Update, 6> second{};

    for (std::size_t i = 0;
         i < second.size();
         ++i) {

        second[i].sequence =
            static_cast<std::uint32_t>(6 + i);
    }

    ASSERT_TRUE(
        queue.try_push_batch(
            second.data(),
            second.size()
        )
    );

    std::array<pulsegrid::Update, 8> output{};

    ASSERT_EQ(
        queue.try_pop_batch(
            output.data(),
            output.size()
        ),
        output.size()
    );

    for (std::size_t i = 0;
         i < output.size();
         ++i) {

        EXPECT_EQ(
            output[i].sequence,
            static_cast<std::uint32_t>(4 + i)
        );
    }
}
