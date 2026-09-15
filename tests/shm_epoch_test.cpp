#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>

#include <gtest/gtest.h>

#include "pulsegrid/shm_queue.hpp"
#include "pulsegrid/transport_frame.hpp"

namespace {

constexpr std::size_t kCapacity = 8;

using Queue = pulsegrid::ShmQueue<
    kCapacity,
    pulsegrid::TransportFrame
>;

} // namespace

TEST(ShmEpoch, InitializeExposesPublicationEpoch) {
    auto memory =
        std::make_unique<std::byte[]>(
            Queue::mapped_size()
        );

    constexpr std::uint64_t kEpoch =
        0x123456789ABCDEF0ULL;

    auto queue =
        Queue::initialize(
            memory.get(),
            kEpoch
        );

    EXPECT_EQ(
        queue.publication_epoch(),
        kEpoch
    );
}

TEST(ShmEpoch, AttachedHandleObservesSameEpoch) {
    auto memory =
        std::make_unique<std::byte[]>(
            Queue::mapped_size()
        );

    constexpr std::uint64_t kEpoch =
        0xCAFEBABE12345678ULL;

    auto producer =
        Queue::initialize(
            memory.get(),
            kEpoch
        );

    auto consumer =
        Queue::attach(memory.get());

    EXPECT_EQ(
        producer.publication_epoch(),
        kEpoch
    );

    EXPECT_EQ(
        consumer.publication_epoch(),
        kEpoch
    );
}

TEST(ShmEpoch, RejectsZeroEpochWithoutInitializingMemory) {
    auto memory =
        std::make_unique<std::byte[]>(
            Queue::mapped_size()
        );

    EXPECT_THROW(
        (void)Queue::initialize(
            memory.get(),
            0
        ),
        std::invalid_argument
    );
}

TEST(ShmEpoch, AttachRejectsZeroEpochMetadata) {
    auto memory =
        std::make_unique<std::byte[]>(
            Queue::mapped_size()
        );

    constexpr std::uint64_t kEpoch = 42;

    (void)Queue::initialize(
        memory.get(),
        kEpoch
    );

    using Layout = Queue::Layout;

    auto* layout =
        reinterpret_cast<Layout*>(
            memory.get()
        );

    layout->header.publication_epoch = 0;

    EXPECT_THROW(
        (void)Queue::attach(memory.get()),
        std::runtime_error
    );
}
