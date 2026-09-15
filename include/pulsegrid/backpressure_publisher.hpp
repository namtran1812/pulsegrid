#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "pulsegrid/coalescing_buffer.hpp"
#include "pulsegrid/frame_codec.hpp"
#include "pulsegrid/shm_queue.hpp"
#include "pulsegrid/transport_frame.hpp"

namespace pulsegrid {

template <std::size_t Capacity>
class BackpressurePublisher {
public:
    using Queue =
        ShmQueue<Capacity, TransportFrame>;

    static_assert(
        Capacity >= 4,
        "BackpressurePublisher requires "
        "at least four queue slots"
    );

    explicit BackpressurePublisher(
        Queue& queue
    ) noexcept
        : queue_(queue),
          pending_(Capacity - 2) {}

    // Returns true if this publication was made
    // visible immediately.
    //
    // Returns false if it was retained in the
    // coalescing buffer because of backpressure.
    [[nodiscard]]
    bool publish(const Update& update) {
        // Once backpressured, preserve ordering:
        // newer publications must join the same
        // pending interval until it is flushed.
        if (!pending_.empty()) {
            pending_.push(update);
            return false;
        }

        const auto frame =
            encode_delta(update);

        if (queue_.try_push(frame)) {
            return true;
        }

        pending_.push(update);
        return false;
    }

    // Attempts to publish the complete pending
    // coalesced transaction. Nothing is removed
    // unless every frame can be committed to the
    // queue in one batch publication.
    [[nodiscard]]
    bool try_flush() {
        if (pending_.empty()) {
            return true;
        }

        const auto batch =
            pending_.snapshot();

        const auto frames =
            encode_coalesced(batch);

        if (
            !queue_.try_push_batch(
                frames.data(),
                frames.size()
            )
        ) {
            return false;
        }

        (void)pending_.flush();
        return true;
    }

    [[nodiscard]]
    bool backpressured() const noexcept {
        return !pending_.empty();
    }

    [[nodiscard]]
    std::size_t pending_cells()
        const noexcept {
        return pending_.size();
    }

    [[nodiscard]]
    std::uint32_t
    pending_watermark() const noexcept {
        const auto sequence =
            pending_.last_sequence();

        return sequence
            ? *sequence
            : 0;
    }

private:
    Queue& queue_;
    CoalescingBuffer pending_;
};

} // namespace pulsegrid
