#pragma once

#include <cstdint>
#include <type_traits>

#include "pulsegrid/update.hpp"

namespace pulsegrid {

enum class FrameKind : std::uint8_t {
    Delta = 0,
    CoalescedBegin = 1,
    CoalescedCell = 2,
    CoalescedEnd = 3
};

struct TransportFrame {
    FrameKind kind{FrameKind::Delta};

    // Inclusive publication range represented
    // by this frame or coalesced batch.
    std::uint64_t first_sequence{0};
    std::uint64_t watermark{0};

    // Number of retained cells in a coalesced
    // batch. Zero for ordinary deltas.
    std::uint32_t cell_count{0};

    // Meaningful for Delta and CoalescedCell.
    Update update{};
};

static_assert(
    std::is_trivially_copyable_v<
        TransportFrame
    >
);

} // namespace pulsegrid
