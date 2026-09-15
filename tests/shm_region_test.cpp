#include <cstddef>
#include <cstdint>
#include <string>
#include <stdexcept>

#include <gtest/gtest.h>

#include <unistd.h>

#include "pulsegrid/shm_region.hpp"

namespace {

std::string unique_name() {
    static std::uint64_t nonce = 0;

    return "/pg_region_" +
           std::to_string(
               static_cast<long long>(::getpid())
           ) +
           "_" +
           std::to_string(++nonce);
}

} // namespace

TEST(ShmRegion, MapsSameMemoryAcrossHandles) {
    constexpr std::size_t kSize = 4096;

    const auto name = unique_name();

    pulsegrid::ShmRegion creator{
        name,
        kSize,
        pulsegrid::ShmRegion::Mode::Create
    };

    auto* creator_value =
        static_cast<std::uint64_t*>(creator.data());

    *creator_value = 0xDEADBEEFCAFEBABEULL;

    pulsegrid::ShmRegion opener{
        name,
        kSize,
        pulsegrid::ShmRegion::Mode::Open
    };

    const auto* opener_value =
        static_cast<const std::uint64_t*>(
            opener.data()
        );

    EXPECT_EQ(
        *opener_value,
        0xDEADBEEFCAFEBABEULL
    );

    creator.unlink();
}

TEST(ShmRegion, RejectsOpenBeyondBackingExtent) {
    constexpr std::size_t kActualSize = 4096;

    // macOS may report a backing extent larger than the
    // logical ftruncate size (for example, 16 KiB for a
    // 4 KiB object). Request well beyond that extent so
    // this test exercises the pre-mmap safety check.
    constexpr std::size_t kRequestedSize =
        1024 * 1024;

    const auto name = unique_name();

    pulsegrid::ShmRegion creator{
        name,
        kActualSize,
        pulsegrid::ShmRegion::Mode::Create
    };

    const auto open_with_wrong_size = [&]() {
        pulsegrid::ShmRegion opener{
            name,
            kRequestedSize,
            pulsegrid::ShmRegion::Mode::Open
        };
    };

    EXPECT_THROW(
        open_with_wrong_size(),
        std::runtime_error
    );

    creator.unlink();
}

TEST(ShmRegion, AllowsOpenWithinLargerBackingObject) {
    constexpr std::size_t kActualSize = 8192;
    constexpr std::size_t kRequestedSize = 4096;

    const auto name = unique_name();

    pulsegrid::ShmRegion creator{
        name,
        kActualSize,
        pulsegrid::ShmRegion::Mode::Create
    };

    auto* creator_value =
        static_cast<std::uint64_t*>(
            creator.data()
        );

    *creator_value = 0x123456789ABCDEF0ULL;

    pulsegrid::ShmRegion opener{
        name,
        kRequestedSize,
        pulsegrid::ShmRegion::Mode::Open
    };

    EXPECT_EQ(
        opener.size(),
        kRequestedSize
    );

    const auto* opener_value =
        static_cast<const std::uint64_t*>(
            opener.data()
        );

    EXPECT_EQ(
        *opener_value,
        0x123456789ABCDEF0ULL
    );

    creator.unlink();
}
