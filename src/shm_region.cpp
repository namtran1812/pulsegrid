#include "pulsegrid/shm_region.hpp"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace pulsegrid {

namespace {

std::runtime_error system_error(const char* operation) {
    return std::runtime_error(
        std::string(operation) + ": " + std::strerror(errno)
    );
}

} // namespace

ShmRegion::ShmRegion(
    std::string name,
    std::size_t size,
    Mode mode
)
    : name_(std::move(name)),
      size_(size),
      owner_(mode == Mode::Create) {

    if (name_.empty() || name_.front() != '/') {
        throw std::invalid_argument(
            "shared-memory name must begin with '/'"
        );
    }

    if (size_ == 0) {
        throw std::invalid_argument(
            "shared-memory size must be non-zero"
        );
    }

    const int flags =
        mode == Mode::Create
            ? O_CREAT | O_EXCL | O_RDWR
            : O_RDWR;

    fd_ = ::shm_open(
        name_.c_str(),
        flags,
        0600
    );

    if (fd_ == -1) {
        throw system_error("shm_open");
    }

    if (mode == Mode::Create) {
        if (::ftruncate(
                fd_,
                static_cast<off_t>(size_)
            ) == -1) {

            const auto error =
                system_error("ftruncate");

            reset();
            ::shm_unlink(name_.c_str());

            throw error;
        }
    }

    address_ = ::mmap(
        nullptr,
        size_,
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        fd_,
        0
    );

    if (address_ == MAP_FAILED) {
        address_ = nullptr;

        const auto error =
            system_error("mmap");

        reset();

        if (owner_) {
            ::shm_unlink(name_.c_str());
        }

        throw error;
    }
}

ShmRegion::~ShmRegion() {
    reset();
}

ShmRegion::ShmRegion(
    ShmRegion&& other
) noexcept
    : name_(std::move(other.name_)),
      size_(other.size_),
      fd_(other.fd_),
      address_(other.address_),
      owner_(other.owner_) {

    other.size_ = 0;
    other.fd_ = -1;
    other.address_ = nullptr;
    other.owner_ = false;
}

ShmRegion& ShmRegion::operator=(
    ShmRegion&& other
) noexcept {

    if (this == &other) {
        return *this;
    }

    reset();

    name_ = std::move(other.name_);
    size_ = other.size_;
    fd_ = other.fd_;
    address_ = other.address_;
    owner_ = other.owner_;

    other.size_ = 0;
    other.fd_ = -1;
    other.address_ = nullptr;
    other.owner_ = false;

    return *this;
}

void ShmRegion::unlink() {
    if (name_.empty()) {
        return;
    }

    if (::shm_unlink(name_.c_str()) == -1 &&
        errno != ENOENT) {

        throw system_error("shm_unlink");
    }

    owner_ = false;
}

void ShmRegion::reset() noexcept {
    if (address_ != nullptr) {
        ::munmap(address_, size_);
        address_ = nullptr;
    }

    if (fd_ != -1) {
        ::close(fd_);
        fd_ = -1;
    }
}

} // namespace pulsegrid
