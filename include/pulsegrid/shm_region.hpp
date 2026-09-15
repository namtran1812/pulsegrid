#pragma once

#include <cstddef>
#include <string>

namespace pulsegrid {

class ShmRegion {
public:
    enum class Mode {
        Create,
        Open
    };

    ShmRegion(
        std::string name,
        std::size_t size,
        Mode mode
    );

    ~ShmRegion();

    ShmRegion(const ShmRegion&) = delete;
    ShmRegion& operator=(const ShmRegion&) = delete;

    ShmRegion(ShmRegion&& other) noexcept;
    ShmRegion& operator=(ShmRegion&& other) noexcept;

    [[nodiscard]]
    void* data() noexcept {
        return address_;
    }

    [[nodiscard]]
    const void* data() const noexcept {
        return address_;
    }

    [[nodiscard]]
    std::size_t size() const noexcept {
        return size_;
    }

    void unlink();

private:
    void reset() noexcept;

    std::string name_;
    std::size_t size_{0};
    int fd_{-1};
    void* address_{nullptr};
    bool owner_{false};
};

} // namespace pulsegrid
