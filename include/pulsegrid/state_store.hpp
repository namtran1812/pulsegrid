#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

#include "pulsegrid/update.hpp"

namespace pulsegrid {

struct CellKey {
    std::uint32_t table_id;
    std::uint32_t row_id;
    std::uint16_t column_id;

    bool operator==(const CellKey&) const noexcept = default;
};

struct CellKeyHash {
    std::size_t operator()(
        const CellKey& key
    ) const noexcept {
        std::size_t seed = key.table_id;

        seed ^= static_cast<std::size_t>(
                    key.row_id
                ) +
                0x9e3779b9 +
                (seed << 6) +
                (seed >> 2);

        seed ^= static_cast<std::size_t>(
                    key.column_id
                ) +
                0x9e3779b9 +
                (seed << 6) +
                (seed >> 2);

        return seed;
    }
};

using CellValue =
    std::variant<
        std::int64_t,
        std::uint64_t,
        double
    >;

struct Cell {
    CellValue value;
    std::uint32_t sequence;
    std::uint64_t timestamp_ns;
};

struct Snapshot {
    std::uint32_t sequence;
    std::vector<Update> updates;
};

class SequenceError
    : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class StaleSequence
    : public SequenceError {
public:
    using SequenceError::SequenceError;
};

class SequenceGap
    : public SequenceError {
public:
    using SequenceError::SequenceError;
};

class StateStore {
public:
    void apply(const Update& update) {
        validate_sequence(update.sequence);

        const CellKey key{
            .table_id = update.table_id,
            .row_id = update.row_id,
            .column_id = update.column_id
        };

        cells_[key] = Cell{
            .value = decode_value(update),
            .sequence = update.sequence,
            .timestamp_ns = update.timestamp_ns
        };

        last_sequence_ = update.sequence;
    }

    [[nodiscard]]
    std::optional<Cell> get(
        std::uint32_t table_id,
        std::uint32_t row_id,
        std::uint16_t column_id
    ) const {
        const CellKey key{
            .table_id = table_id,
            .row_id = row_id,
            .column_id = column_id
        };

        const auto it =
            cells_.find(key);

        if (it == cells_.end()) {
            return std::nullopt;
        }

        return it->second;
    }

    [[nodiscard]]
    std::size_t size() const noexcept {
        return cells_.size();
    }

    [[nodiscard]]
    std::optional<std::uint32_t>
    last_sequence() const noexcept {
        return last_sequence_;
    }

    [[nodiscard]]
    static StateStore restore(
        const Snapshot& snapshot
    ) {
        StateStore store;

        if (
            snapshot.sequence == 0 &&
            !snapshot.updates.empty()
        ) {
            throw std::runtime_error(
                "non-empty snapshot has zero sequence"
            );
        }

        for (const auto& update :
             snapshot.updates) {

            if (
                update.sequence >
                snapshot.sequence
            ) {
                throw std::runtime_error(
                    "snapshot cell exceeds "
                    "snapshot sequence"
                );
            }

            const CellKey key{
                .table_id = update.table_id,
                .row_id = update.row_id,
                .column_id = update.column_id
            };

            if (store.cells_.contains(key)) {
                throw std::runtime_error(
                    "duplicate cell in snapshot"
                );
            }

            store.cells_.emplace(
                key,
                Cell{
                    .value =
                        decode_value(update),
                    .sequence =
                        update.sequence,
                    .timestamp_ns =
                        update.timestamp_ns
                }
            );
        }

        if (snapshot.sequence != 0) {
            store.last_sequence_ =
                snapshot.sequence;
        }

        return store;
    }

    [[nodiscard]]
    Snapshot snapshot() const {
        Snapshot result{
            .sequence =
                last_sequence_.value_or(0),
            .updates = {}
        };

        result.updates.reserve(
            cells_.size()
        );

        for (const auto& [key, cell] : cells_) {
            result.updates.push_back(
                encode_cell(key, cell)
            );
        }

        return result;
    }

private:
    void validate_sequence(
        std::uint32_t sequence
    ) const {
        if (!last_sequence_) {
            return;
        }

        if (sequence <= *last_sequence_) {
            throw StaleSequence(
                "duplicate or stale stream sequence"
            );
        }

        const auto expected =
            static_cast<std::uint32_t>(
                *last_sequence_ + 1
            );

        if (sequence != expected) {
            throw SequenceGap(
                "gap in publication sequence"
            );
        }
    }

    static CellValue decode_value(
        const Update& update
    ) {
        switch (update.type) {
        case ValueType::Int64:
            return std::bit_cast<std::int64_t>(
                update.payload
            );

        case ValueType::UInt64:
            return update.payload;

        case ValueType::Double:
            return std::bit_cast<double>(
                update.payload
            );
        }

        throw std::runtime_error(
            "unknown value type"
        );
    }

    static Update encode_cell(
        const CellKey& key,
        const Cell& cell
    ) {
        Update update{
            .table_id = key.table_id,
            .row_id = key.row_id,
            .column_id = key.column_id,
            .type = ValueType::UInt64,
            .sequence = cell.sequence,
            .timestamp_ns =
                cell.timestamp_ns,
            .payload = 0
        };

        std::visit(
            [&](const auto& value) {
                using T =
                    std::decay_t<
                        decltype(value)
                    >;

                if constexpr (
                    std::is_same_v<
                        T,
                        std::int64_t
                    >
                ) {
                    update.type =
                        ValueType::Int64;

                    update.payload =
                        std::bit_cast<
                            std::uint64_t
                        >(value);
                }
                else if constexpr (
                    std::is_same_v<
                        T,
                        std::uint64_t
                    >
                ) {
                    update.type =
                        ValueType::UInt64;

                    update.payload =
                        value;
                }
                else {
                    update.type =
                        ValueType::Double;

                    update.payload =
                        std::bit_cast<
                            std::uint64_t
                        >(value);
                }
            },
            cell.value
        );

        return update;
    }

    std::unordered_map<
        CellKey,
        Cell,
        CellKeyHash
    > cells_;

    std::optional<std::uint32_t>
        last_sequence_;
};

} // namespace pulsegrid
