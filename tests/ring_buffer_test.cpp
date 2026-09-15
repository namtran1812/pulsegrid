#include <cstdint>

#include <gtest/gtest.h>

#include "pulsegrid/ring_buffer.hpp"

TEST(RingBuffer, StartsEmpty) {
    pulsegrid::SpscRingBuffer<std::uint64_t, 8> queue;

    EXPECT_TRUE(queue.empty());

    std::uint64_t value = 0;
    EXPECT_FALSE(queue.try_pop(value));
}

TEST(RingBuffer, PushThenPop) {
    pulsegrid::SpscRingBuffer<std::uint64_t, 8> queue;

    EXPECT_TRUE(queue.try_push(42));

    std::uint64_t value = 0;

    EXPECT_TRUE(queue.try_pop(value));
    EXPECT_EQ(value, 42);
    EXPECT_TRUE(queue.empty());
}

TEST(RingBuffer, PreservesFIFOOrder) {
    pulsegrid::SpscRingBuffer<std::uint64_t, 8> queue;

    for (std::uint64_t i = 0; i < 8; ++i) {
        EXPECT_TRUE(queue.try_push(i));
    }

    std::uint64_t value = 0;

    for (std::uint64_t i = 0; i < 8; ++i) {
        EXPECT_TRUE(queue.try_pop(value));
        EXPECT_EQ(value, i);
    }
}

TEST(RingBuffer, RejectsPushWhenFull) {
    pulsegrid::SpscRingBuffer<std::uint64_t, 4> queue;

    EXPECT_TRUE(queue.try_push(1));
    EXPECT_TRUE(queue.try_push(2));
    EXPECT_TRUE(queue.try_push(3));
    EXPECT_TRUE(queue.try_push(4));

    EXPECT_FALSE(queue.try_push(5));
}

#include <atomic>
#include <thread>

TEST(RingBuffer, ConcurrentProducerConsumer) {
    constexpr std::uint64_t kMessageCount = 5'000'000;

    pulsegrid::SpscRingBuffer<std::uint64_t, 1024> queue;

    std::atomic<bool> start{false};

    std::thread producer([&] {
        while (!start.load(std::memory_order_acquire)) {
        }

        for (std::uint64_t i = 0; i < kMessageCount; ++i) {
            while (!queue.try_push(i)) {
            }
        }
    });

    std::thread consumer([&] {
        start.store(true, std::memory_order_release);

        for (std::uint64_t expected = 0;
             expected < kMessageCount;
             ++expected) {

            std::uint64_t value = 0;

            while (!queue.try_pop(value)) {
            }

            ASSERT_EQ(value, expected);
        }
    });

    producer.join();
    consumer.join();

    EXPECT_TRUE(queue.empty());
}
