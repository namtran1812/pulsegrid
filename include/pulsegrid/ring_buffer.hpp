#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <type_traits>

namespace pulsegrid {

inline constexpr std::size_t kCacheLineSize = 64;

template <typename T, std::size_t Capacity>
class SpscRingBuffer {
    static_assert(Capacity >= 2);
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>,
                  "SPSC payload must be trivially copyable");

public:
    static constexpr std::size_t capacity() noexcept {
        return Capacity;
    }

    [[nodiscard]]
    bool try_push(const T& value) noexcept {
        const std::size_t head =
            head_.value.load(std::memory_order_relaxed);

        const std::size_t tail =
            tail_.value.load(std::memory_order_acquire);

        if (head - tail == Capacity) {
            return false;
        }

        buffer_[head & kMask] = value;

        head_.value.store(head + 1, std::memory_order_release);

        return true;
    }

    [[nodiscard]]
    bool try_pop(T& value) noexcept {
        const std::size_t tail =
            tail_.value.load(std::memory_order_relaxed);

        const std::size_t head =
            head_.value.load(std::memory_order_acquire);

        if (tail == head) {
            return false;
        }

        value = buffer_[tail & kMask];

        tail_.value.store(tail + 1, std::memory_order_release);

        return true;
    }

    [[nodiscard]]
    bool empty() const noexcept {
        return head_.value.load(std::memory_order_acquire) ==
               tail_.value.load(std::memory_order_acquire);
    }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    struct alignas(kCacheLineSize) Cursor {
        std::atomic<std::size_t> value{0};
    };

    Cursor head_;
    Cursor tail_;

    alignas(kCacheLineSize)
    std::array<T, Capacity> buffer_{};
};

} // namespace pulsegrid
