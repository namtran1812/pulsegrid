#include <cstdint>

#include <gtest/gtest.h>

#include "pulsegrid/ring_buffer.hpp"

TEST(RingBufferWraparound, ReusesSlotsCorrectly) {
    pulsegrid::SpscRingBuffer<std::uint64_t, 4> queue;

    std::uint64_t value = 0;

    for (std::uint64_t round = 0; round < 1000; ++round) {
        for (std::uint64_t i = 0; i < 4; ++i) {
            const auto expected = round * 4 + i;
            ASSERT_TRUE(queue.try_push(expected));
        }

        for (std::uint64_t i = 0; i < 4; ++i) {
            const auto expected = round * 4 + i;

            ASSERT_TRUE(queue.try_pop(value));
            ASSERT_EQ(value, expected);
        }

        ASSERT_TRUE(queue.empty());
    }
}
