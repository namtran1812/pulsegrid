#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "pulsegrid/coalesced_batch.hpp"
#include "pulsegrid/state_store.hpp"
#include "pulsegrid/update.hpp"

namespace pulsegrid {

class CoalescingOverflow :
    public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class CoalescingBuffer {
public:
    explicit CoalescingBuffer(
        std::size_t max_cells =
            std::numeric_limits<
                std::size_t
            >::max()
    ) noexcept
        : max_cells_(max_cells) {}

    void push(const Update& update) {
        validate_sequence(update.sequence);

        const CellKey key{
            .table_id = update.table_id,
            .row_id = update.row_id,
            .column_id = update.column_id
        };

        // Replacing an already-retained cell does
        // not increase the eventual transaction
        // size, so it remains legal at capacity.
        if (
            !pending_.contains(key) &&
            pending_.size() >= max_cells_
        ) {
            throw CoalescingOverflow(
                "coalescing buffer capacity "
                "exceeded"
            );
        }

        // No state is mutated before all validation
        // above succeeds.
        if (pending_.empty()) {
            first_pending_sequence_ =
                update.sequence;
        }

        // Replacing an existing entry intentionally
        // discards an obsolete intermediate value.
        pending_[key] = update;

        last_sequence_ = update.sequence;
    }

    [[nodiscard]]
    std::size_t size() const noexcept {
        return pending_.size();
    }

    [[nodiscard]]
    bool empty() const noexcept {
        return pending_.empty();
    }

    [[nodiscard]]
    std::optional<std::uint64_t>
    last_sequence() const noexcept {
        return last_sequence_;
    }

    [[nodiscard]]
    CoalescedBatch snapshot() const {
        if (pending_.empty()) {
            return {
                .first_sequence = 0,
                .watermark = 0,
                .updates = {}
            };
        }

        CoalescedBatch batch{
            .first_sequence =
                *first_pending_sequence_,
            .watermark =
                *last_sequence_,
            .updates = {}
        };

        batch.updates.reserve(
            pending_.size()
        );

        for (const auto& [key, update] :
             pending_) {
            (void)key;
            batch.updates.push_back(
                update
            );
        }

        std::sort(
            batch.updates.begin(),
            batch.updates.end(),
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

        return batch;
    }

    [[nodiscard]]
    CoalescedBatch flush() {
        auto batch = snapshot();

        if (pending_.empty()) {
            return batch;
        }

        pending_.clear();
        first_pending_sequence_.reset();

        return batch;
    }

private:
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
                "duplicate or stale coalescing sequence"
            );
        }

        const auto expected =
            *last_sequence_ + 1;

        if (sequence != expected) {
            throw SequenceGap(
                "gap in coalescing input sequence"
            );
        }
    }

    std::unordered_map<
        CellKey,
        Update,
        CellKeyHash
    > pending_;

    std::size_t max_cells_;

    std::optional<std::uint64_t>
        last_sequence_;

    std::optional<std::uint64_t>
        first_pending_sequence_;
};

} // namespace pulsegrid
