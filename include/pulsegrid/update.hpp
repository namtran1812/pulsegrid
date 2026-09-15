#pragma once

#include <cstdint>
#include <type_traits>

namespace pulsegrid {

enum class ValueType : std::uint16_t {
    Int64 = 0,
    UInt64 = 1,
    Double = 2
};

struct alignas(32) Update {
    std::uint32_t table_id;
    std::uint32_t row_id;

    std::uint16_t column_id;
    ValueType type;

    std::uint32_t sequence;

    std::uint64_t timestamp_ns;
    std::uint64_t payload;
};

static_assert(sizeof(Update) == 32);
static_assert(std::is_trivially_copyable_v<Update>);

} // namespace pulsegrid
