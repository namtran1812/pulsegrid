#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <stdexcept>

#include "pulsegrid/update.hpp"

namespace pulsegrid {

inline constexpr std::uint64_t kShmMagic =
    0x50554C5345475244ULL;

inline constexpr std::uint32_t kShmVersion = 1;
inline constexpr std::size_t kShmCacheLine = 64;

enum class InitState : std::uint32_t {
    Uninitialized = 0,
    Initializing = 1,
    Ready = 2
};

enum class ConsumerState : std::uint32_t {
    Disconnected = 0,
    Attached = 1,
    Complete = 2
};

struct alignas(kShmCacheLine) ShmHeader {
    std::uint64_t magic{0};
    std::uint32_t version{0};
    std::uint32_t capacity{0};

    std::atomic<std::uint32_t> init_state{
        static_cast<std::uint32_t>(
            InitState::Uninitialized
        )
    };

    std::atomic<std::uint32_t> consumer_state{
        static_cast<std::uint32_t>(
            ConsumerState::Disconnected
        )
    };
};

struct alignas(kShmCacheLine) ShmCursor {
    std::atomic<std::uint64_t> value{0};
};

template <std::size_t Capacity>
struct ShmQueueLayout {
    static_assert(Capacity >= 2);
    static_assert(
        (Capacity & (Capacity - 1)) == 0,
        "Capacity must be a power of two"
    );

    ShmHeader header;
    ShmCursor head;
    ShmCursor tail;

    alignas(kShmCacheLine)
    Update entries[Capacity];
};

template <std::size_t Capacity>
class ShmQueue {
public:
    using Layout = ShmQueueLayout<Capacity>;

    static_assert(
        std::atomic<std::uint64_t>::is_always_lock_free,
        "PulseGrid requires lock-free 64-bit atomics"
    );

    static_assert(
        std::atomic<std::uint32_t>::is_always_lock_free,
        "PulseGrid requires lock-free 32-bit atomics"
    );

    static constexpr std::size_t mapped_size() noexcept {
        return sizeof(Layout);
    }

    static ShmQueue initialize(void* memory) {
        if (memory == nullptr) {
            throw std::invalid_argument(
                "null shared memory"
            );
        }

        auto* layout = ::new (memory) Layout{};

        layout->header.init_state.store(
            static_cast<std::uint32_t>(
                InitState::Initializing
            ),
            std::memory_order_relaxed
        );

        layout->header.magic = kShmMagic;
        layout->header.version = kShmVersion;
        layout->header.capacity =
            static_cast<std::uint32_t>(Capacity);

        layout->head.value.store(
            0,
            std::memory_order_relaxed
        );

        layout->tail.value.store(
            0,
            std::memory_order_relaxed
        );

        layout->header.consumer_state.store(
            static_cast<std::uint32_t>(
                ConsumerState::Disconnected
            ),
            std::memory_order_relaxed
        );

        layout->header.init_state.store(
            static_cast<std::uint32_t>(
                InitState::Ready
            ),
            std::memory_order_release
        );

        return ShmQueue(layout);
    }

    static ShmQueue attach(void* memory) {
        if (memory == nullptr) {
            throw std::invalid_argument(
                "null shared memory"
            );
        }

        auto* layout =
            static_cast<Layout*>(memory);

        const auto state =
            layout->header.init_state.load(
                std::memory_order_acquire
            );

        if (state !=
            static_cast<std::uint32_t>(
                InitState::Ready
            )) {
            throw std::runtime_error(
                "shared queue is not ready"
            );
        }

        if (layout->header.magic != kShmMagic) {
            throw std::runtime_error(
                "invalid shared-memory magic"
            );
        }

        if (layout->header.version != kShmVersion) {
            throw std::runtime_error(
                "unsupported shared-memory version"
            );
        }

        if (layout->header.capacity != Capacity) {
            throw std::runtime_error(
                "shared-memory capacity mismatch"
            );
        }

        return ShmQueue(layout);
    }

    [[nodiscard]]
    bool try_push(const Update& update) noexcept {
        const auto head =
            layout_->head.value.load(
                std::memory_order_relaxed
            );

        const auto tail =
            layout_->tail.value.load(
                std::memory_order_acquire
            );

        if (head - tail == Capacity) {
            return false;
        }

        layout_->entries[
            static_cast<std::size_t>(head) & kMask
        ] = update;

        layout_->head.value.store(
            head + 1,
            std::memory_order_release
        );

        return true;
    }

    [[nodiscard]]
    bool try_push_batch(
        const Update* updates,
        std::size_t count
    ) noexcept {
        if (count == 0) {
            return true;
        }

        if (count > Capacity) {
            return false;
        }

        const auto head =
            layout_->head.value.load(
                std::memory_order_relaxed
            );

        const auto tail =
            layout_->tail.value.load(
                std::memory_order_acquire
            );

        const auto used = head - tail;
        const auto available = Capacity - used;

        if (count > available) {
            return false;
        }

        for (std::size_t i = 0;
             i < count;
             ++i) {

            layout_->entries[
                static_cast<std::size_t>(
                    head + i
                ) & kMask
            ] = updates[i];
        }

        layout_->head.value.store(
            head + count,
            std::memory_order_release
        );

        return true;
    }

    [[nodiscard]]
    std::size_t try_pop_batch(
        Update* updates,
        std::size_t max_count
    ) noexcept {
        if (max_count == 0) {
            return 0;
        }

        const auto tail =
            layout_->tail.value.load(
                std::memory_order_relaxed
            );

        const auto head =
            layout_->head.value.load(
                std::memory_order_acquire
            );

        const auto available = head - tail;

        const auto count =
            static_cast<std::size_t>(
                available < max_count
                    ? available
                    : max_count
            );

        if (count == 0) {
            return 0;
        }

        for (std::size_t i = 0;
             i < count;
             ++i) {

            updates[i] =
                layout_->entries[
                    static_cast<std::size_t>(
                        tail + i
                    ) & kMask
                ];
        }

        layout_->tail.value.store(
            tail + count,
            std::memory_order_release
        );

        return count;
    }

    [[nodiscard]]
    bool try_pop(Update& update) noexcept {
        const auto tail =
            layout_->tail.value.load(
                std::memory_order_relaxed
            );

        const auto head =
            layout_->head.value.load(
                std::memory_order_acquire
            );

        if (tail == head) {
            return false;
        }

        update =
            layout_->entries[
                static_cast<std::size_t>(tail) & kMask
            ];

        layout_->tail.value.store(
            tail + 1,
            std::memory_order_release
        );

        return true;
    }

    void mark_consumer_attached() noexcept {
        layout_->header.consumer_state.store(
            static_cast<std::uint32_t>(
                ConsumerState::Attached
            ),
            std::memory_order_release
        );
    }

    void mark_consumer_complete() noexcept {
        layout_->header.consumer_state.store(
            static_cast<std::uint32_t>(
                ConsumerState::Complete
            ),
            std::memory_order_release
        );
    }

    [[nodiscard]]
    bool consumer_complete() const noexcept {
        return layout_->header.consumer_state.load(
            std::memory_order_acquire
        ) ==
        static_cast<std::uint32_t>(
            ConsumerState::Complete
        );
    }

private:
    static constexpr std::size_t kMask =
        Capacity - 1;

    explicit ShmQueue(Layout* layout) noexcept
        : layout_(layout) {}

    Layout* layout_;
};

} // namespace pulsegrid
