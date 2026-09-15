#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <vector>

#include "pulsegrid/coalesced_batch.hpp"
#include "pulsegrid/transport_frame.hpp"

namespace pulsegrid {

inline TransportFrame encode_delta(
    const Update& update
) {
    return {
        .kind = FrameKind::Delta,
        .first_sequence = update.sequence,
        .watermark = update.sequence,
        .cell_count = 0,
        .update = update
    };
}

inline std::vector<TransportFrame>
encode_coalesced(
    const CoalescedBatch& batch
) {
    if (
        batch.updates.empty() ||
        batch.first_sequence == 0 ||
        batch.first_sequence >
            batch.watermark
    ) {
        throw std::runtime_error(
            "invalid coalesced batch"
        );
    }

    const auto count =
        static_cast<std::uint32_t>(
            batch.updates.size()
        );

    std::vector<TransportFrame> frames;
    frames.reserve(
        batch.updates.size() + 2
    );

    frames.push_back({
        .kind = FrameKind::CoalescedBegin,
        .first_sequence =
            batch.first_sequence,
        .watermark = batch.watermark,
        .cell_count = count,
        .update = {}
    });

    for (const auto& update :
         batch.updates) {
        frames.push_back({
            .kind = FrameKind::CoalescedCell,
            .first_sequence =
                batch.first_sequence,
            .watermark =
                batch.watermark,
            .cell_count = count,
            .update = update
        });
    }

    frames.push_back({
        .kind = FrameKind::CoalescedEnd,
        .first_sequence =
            batch.first_sequence,
        .watermark = batch.watermark,
        .cell_count = count,
        .update = {}
    });

    return frames;
}

class FrameDecoder {
public:
    struct Result {
        std::optional<Update> delta;
        std::optional<CoalescedBatch>
            coalesced;
    };

    [[nodiscard]]
    Result consume(
        const TransportFrame& frame
    ) {
        switch (frame.kind) {
        case FrameKind::Delta:
            return consume_delta(frame);

        case FrameKind::CoalescedBegin:
            consume_begin(frame);
            return {};

        case FrameKind::CoalescedCell:
            consume_cell(frame);
            return {};

        case FrameKind::CoalescedEnd:
            return consume_end(frame);
        }

        throw std::runtime_error(
            "unknown transport frame kind"
        );
    }

    [[nodiscard]]
    bool in_batch() const noexcept {
        return active_;
    }

    void reset() noexcept {
        active_ = false;
        first_sequence_ = 0;
        watermark_ = 0;
        expected_count_ = 0;
        updates_.clear();
    }

private:
    Result consume_delta(
        const TransportFrame& frame
    ) {
        if (active_) {
            throw std::runtime_error(
                "delta encountered inside "
                "coalesced batch"
            );
        }

        if (
            frame.first_sequence !=
                frame.update.sequence ||
            frame.watermark !=
                frame.update.sequence ||
            frame.cell_count != 0
        ) {
            throw std::runtime_error(
                "invalid delta frame"
            );
        }

        return {
            .delta = frame.update,
            .coalesced = std::nullopt
        };
    }

    void consume_begin(
        const TransportFrame& frame
    ) {
        if (active_) {
            throw std::runtime_error(
                "nested coalesced batch"
            );
        }

        if (
            frame.first_sequence == 0 ||
            frame.first_sequence >
                frame.watermark ||
            frame.cell_count == 0
        ) {
            throw std::runtime_error(
                "invalid coalesced begin frame"
            );
        }

        active_ = true;
        first_sequence_ =
            frame.first_sequence;
        watermark_ = frame.watermark;
        expected_count_ =
            frame.cell_count;

        updates_.clear();
        updates_.reserve(
            expected_count_
        );
    }

    void consume_cell(
        const TransportFrame& frame
    ) {
        require_matching_batch(frame);

        if (
            updates_.size() >=
            expected_count_
        ) {
            throw std::runtime_error(
                "too many coalesced cells"
            );
        }

        if (
            frame.update.sequence <
                first_sequence_ ||
            frame.update.sequence >
                watermark_
        ) {
            throw std::runtime_error(
                "coalesced cell sequence "
                "outside represented range"
            );
        }

        updates_.push_back(
            frame.update
        );
    }

    Result consume_end(
        const TransportFrame& frame
    ) {
        require_matching_batch(frame);

        if (
            updates_.size() !=
            expected_count_
        ) {
            throw std::runtime_error(
                "incomplete coalesced batch"
            );
        }

        CoalescedBatch batch{
            .first_sequence =
                first_sequence_,
            .watermark = watermark_,
            .updates = std::move(updates_)
        };

        reset();

        return {
            .delta = std::nullopt,
            .coalesced =
                std::move(batch)
        };
    }

    void require_matching_batch(
        const TransportFrame& frame
    ) const {
        if (!active_) {
            throw std::runtime_error(
                "coalesced frame without begin"
            );
        }

        if (
            frame.first_sequence !=
                first_sequence_ ||
            frame.watermark !=
                watermark_ ||
            frame.cell_count !=
                expected_count_
        ) {
            throw std::runtime_error(
                "coalesced frame metadata "
                "mismatch"
            );
        }
    }

    bool active_{false};

    std::uint64_t first_sequence_{0};
    std::uint64_t watermark_{0};
    std::size_t expected_count_{0};

    std::vector<Update> updates_;
};

} // namespace pulsegrid
