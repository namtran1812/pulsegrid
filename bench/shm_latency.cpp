#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include "benchmark_common.hpp"
#include "pulsegrid/shm_queue.hpp"
#include "pulsegrid/shm_region.hpp"

namespace {

constexpr std::size_t kCapacity = 1024;

constexpr std::uint64_t kWarmupSamples = 10'000;
constexpr std::uint64_t kMeasuredSamples = 1'000'000;

struct alignas(64) Ack {
    std::atomic<std::uint64_t> sequence{0};
};

using Queue = pulsegrid::ShmQueue<kCapacity>;

constexpr std::size_t align_up(
    std::size_t value,
    std::size_t alignment
) {
    return (value + alignment - 1) &
           ~(alignment - 1);
}

constexpr std::size_t kAckOffset =
    align_up(
        Queue::mapped_size(),
        alignof(Ack)
    );

constexpr std::size_t kMappedSize =
    kAckOffset + sizeof(Ack);

std::string unique_shm_name() {
    return "/pulsegrid_latency_" +
           std::to_string(
               static_cast<long long>(::getpid())
           );
}

Ack* ack_from(void* memory) {
    auto* bytes =
        static_cast<std::byte*>(memory);

    return reinterpret_cast<Ack*>(
        bytes + kAckOffset
    );
}

pulsegrid::Update make_update(
    std::uint32_t sequence
) {
    return {
        .table_id = 1,
        .row_id = sequence % 1000,
        .column_id = 1,
        .type = pulsegrid::ValueType::UInt64,
        .sequence = sequence,
        .timestamp_ns = 0,
        .payload = sequence
    };
}

} // namespace

int main() {
    static_assert(
        std::atomic<std::uint64_t>::is_always_lock_free
    );

    const std::string name =
        unique_shm_name();

    try {
        pulsegrid::ShmRegion region{
            name,
            kMappedSize,
            pulsegrid::ShmRegion::Mode::Create
        };

        auto producer =
            Queue::initialize(region.data());

        auto* ack =
            ::new (ack_from(region.data())) Ack{};

        const pid_t child = ::fork();

        if (child == -1) {
            region.unlink();
            throw std::runtime_error(
                "fork failed"
            );
        }

        if (child == 0) {
            try {
                pulsegrid::ShmRegion child_region{
                    name,
                    kMappedSize,
                    pulsegrid::ShmRegion::Mode::Open
                };

                auto consumer =
                    Queue::attach(
                        child_region.data()
                    );

                auto* child_ack =
                    ack_from(child_region.data());

                const std::uint64_t total =
                    kWarmupSamples +
                    kMeasuredSamples;

                for (std::uint64_t i = 1;
                     i <= total;
                     ++i) {

                    pulsegrid::Update update{};

                    while (!consumer.try_pop(update)) {
                        std::this_thread::yield();
                    }

                    if (update.sequence !=
                            static_cast<std::uint32_t>(i) ||
                        update.payload != i) {

                        std::_Exit(2);
                    }

                    child_ack->sequence.store(
                        i,
                        std::memory_order_release
                    );
                }

                std::_Exit(0);
            }
            catch (...) {
                std::_Exit(3);
            }
        }

        // Warm-up.
        for (std::uint64_t i = 1;
             i <= kWarmupSamples;
             ++i) {

            const auto update =
                make_update(
                    static_cast<std::uint32_t>(i)
                );

            while (!producer.try_push(update)) {
                std::this_thread::yield();
            }

            while (ack->sequence.load(
                       std::memory_order_acquire
                   ) != i) {

                std::this_thread::yield();
            }
        }

        std::vector<std::uint64_t> samples;
        samples.reserve(kMeasuredSamples);

        for (std::uint64_t sample = 0;
             sample < kMeasuredSamples;
             ++sample) {

            const std::uint64_t sequence =
                kWarmupSamples + sample + 1;

            const auto update =
                make_update(
                    static_cast<std::uint32_t>(
                        sequence
                    )
                );

            const auto start =
                pulsegrid::bench::Clock::now();

            while (!producer.try_push(update)) {
                std::this_thread::yield();
            }

            while (ack->sequence.load(
                       std::memory_order_acquire
                   ) != sequence) {

                std::this_thread::yield();
            }

            const auto end =
                pulsegrid::bench::Clock::now();

            const auto ns =
                std::chrono::duration_cast<
                    std::chrono::nanoseconds
                >(end - start).count();

            samples.push_back(
                static_cast<std::uint64_t>(ns)
            );
        }

        int status = 0;

        if (::waitpid(child, &status, 0) != child) {
            region.unlink();

            throw std::runtime_error(
                "waitpid failed"
            );
        }

        region.unlink();

        if (!WIFEXITED(status) ||
            WEXITSTATUS(status) != 0) {

            std::cerr
                << "consumer failed with status "
                << status
                << '\n';

            return 2;
        }

        const auto summary =
            pulsegrid::bench::summarize_latency(
                std::move(samples)
            );

        std::cout
            << "measurement=round_trip\n"
            << "samples="
            << kMeasuredSamples
            << '\n';

        pulsegrid::bench::print_latency(
            "shm",
            summary
        );
    }
    catch (const std::exception& error) {
        std::cerr
            << "benchmark error: "
            << error.what()
            << '\n';

        return 1;
    }

    return 0;
}
