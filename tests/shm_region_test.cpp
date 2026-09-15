#include <cstddef>
#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include <unistd.h>

#include "pulsegrid/shm_region.hpp"

namespace {

std::string unique_name() {
    return "/pulsegrid_test_" +
           std::to_string(
               static_cast<long long>(::getpid())
           );
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
