#pragma once

#include <cstdint>
#include <vector>

#include "pulsegrid/update.hpp"

namespace pulsegrid {

struct CoalescedBatch {
    // First publication represented by this batch.
    std::uint64_t first_sequence;

    // Every publication in the inclusive range
    // [first_sequence, watermark] was observed by
    // the producer before coalescing.
    std::uint64_t watermark;

    // At most one latest update per changed cell.
    std::vector<Update> updates;
};

} // namespace pulsegrid
