#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

#include "pulsegrid/coalesced_batch.hpp"
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
    std::uint64_t sequence;
};

struct Snapshot {
    std::uint64_t epoch;
    std::uint64_t sequence;
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

class EpochMismatch
    : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class StateStore {
public:
    static constexpr std::uint64_t
        kLegacyEpoch = 1;

    void apply(const Update& update) {
        apply(kLegacyEpoch, update);
    }

    void apply(
        std::uint64_t epoch,
        const Update& update
    ) {
        validate_epoch(epoch);
        validate_sequence(update.sequence);

        const CellKey key{
            .table_id = update.table_id,
            .row_id = update.row_id,
            .column_id = update.column_id
        };

        cells_[key] = Cell{
            .value = decode_value(update),
            .sequence = update.sequence,
        };

        epoch_ = epoch;
        last_sequence_ = update.sequence;
    }

    void apply_coalesced(
        const CoalescedBatch& batch
    ) {
        apply_coalesced(kLegacyEpoch, batch);
    }

    void apply_coalesced(
        std::uint64_t epoch,
        const CoalescedBatch& batch
    ) {
        validate_epoch(epoch);
        if (batch.updates.empty()) {
            if (batch.watermark != 0) {
                throw std::runtime_error(
                    "nonzero coalesced watermark "
                    "has no updates"
                );
            }

            return;
        }

        if (
            batch.first_sequence == 0 ||
            batch.first_sequence >
                batch.watermark
        ) {
            throw std::runtime_error(
                "invalid coalesced sequence range"
            );
        }

        if (last_sequence_) {
            const auto expected =
                *last_sequence_ + 1;

            if (
                batch.first_sequence != expected
            ) {
                throw SequenceGap(
                    "coalesced batch does not "
                    "continue current stream"
                );
            }
        }

        if (
            last_sequence_ &&
            batch.watermark <= *last_sequence_
        ) {
            throw StaleSequence(
                "stale coalesced watermark"
            );
        }

        std::unordered_map<
            CellKey,
            bool,
            CellKeyHash
        > seen;

        seen.reserve(batch.updates.size());

        bool contains_watermark = false;

        for (const auto& update :
             batch.updates) {

            if (update.sequence == batch.watermark) {
                contains_watermark = true;
            }

            if (
                update.sequence >
                batch.watermark
            ) {
                throw std::runtime_error(
                    "coalesced update exceeds "
                    "batch watermark"
                );
            }

            if (
                last_sequence_ &&
                update.sequence <=
                    *last_sequence_
            ) {
                throw StaleSequence(
                    "coalesced update is not "
                    "newer than current state"
                );
            }

            const CellKey key{
                .table_id = update.table_id,
                .row_id = update.row_id,
                .column_id = update.column_id
            };

            if (seen.contains(key)) {
                throw std::runtime_error(
                    "duplicate cell in "
                    "coalesced batch"
                );
            }

            seen.emplace(key, true);

            // Validate type before mutating anything.
            (void)decode_value(update);
        }

        if (!contains_watermark) {
            throw std::runtime_error(
                "coalesced batch does not contain "
                "its watermark update"
            );
        }

        for (const auto& update :
             batch.updates) {

            const CellKey key{
                .table_id = update.table_id,
                .row_id = update.row_id,
                .column_id = update.column_id
            };

            cells_[key] = Cell{
                .value = decode_value(update),
                .sequence = update.sequence,
            };
        }

        epoch_ = epoch;
        last_sequence_ = batch.watermark;
    }

    [[nodiscard]]
    std::optional<std::uint64_t>
    epoch() const noexcept {
        return epoch_;
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
    std::optional<std::uint64_t>
    last_sequence() const noexcept {
        return last_sequence_;
    }

    [[nodiscard]]
    static StateStore restore(
        const Snapshot& snapshot
    ) {
        StateStore store;

        if (
            snapshot.epoch == 0 &&
            snapshot.sequence == 0 &&
            snapshot.updates.empty()
        ) {
            return store;
        }

        if (snapshot.epoch == 0) {
            throw std::runtime_error(
                "non-empty publication history has zero epoch"
            );
        }

        if (snapshot.sequence == 0) {
            throw std::runtime_error(
                "non-empty publication history has zero sequence"
            );
        }

        if (snapshot.updates.empty()) {
            throw std::runtime_error(
                "nonzero snapshot watermark has no state"
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
                }
            );
        }

        store.epoch_ = snapshot.epoch;
        store.last_sequence_ = snapshot.sequence;

        return store;
    }

    [[nodiscard]]
    Snapshot snapshot() const {
        Snapshot result{
            .epoch = epoch_.value_or(0),
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

        // Snapshots have a canonical wire order
        // independent of unordered_map iteration.
        std::sort(
            result.updates.begin(),
            result.updates.end(),
            [](const Update& lhs,
               const Update& rhs) {
                if (
                    lhs.table_id !=
                    rhs.table_id
                ) {
                    return lhs.table_id <
                           rhs.table_id;
                }

                if (
                    lhs.row_id !=
                    rhs.row_id
                ) {
                    return lhs.row_id <
                           rhs.row_id;
                }

                return lhs.column_id <
                       rhs.column_id;
            }
        );

        return result;
    }

private:
    void validate_epoch(
        std::uint64_t epoch
    ) const {
        if (epoch == 0) {
            throw EpochMismatch(
                "epoch zero is reserved"
            );
        }

        if (
            epoch_ &&
            epoch != *epoch_
        ) {
            throw EpochMismatch(
                "publication epoch does not match current state"
            );
        }
    }

    void validate_sequence(
        std::uint64_t sequence
    ) const {
        if (sequence == 0) {
            throw SequenceError(
                "sequence zero is reserved"
            );
        }

        if (!last_sequence_) {
            return;
        }

        if (sequence <= *last_sequence_) {
            throw StaleSequence(
                "duplicate or stale stream sequence"
            );
        }

        const auto expected =
            *last_sequence_ + 1;

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

    std::optional<std::uint64_t>
        epoch_;

    std::optional<std::uint64_t>
        last_sequence_;
};

} // namespace pulsegrid
