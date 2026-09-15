#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include "pulsegrid/shm_queue.hpp"
#include "pulsegrid/shm_region.hpp"

namespace {

constexpr std::size_t kCapacity = 4096;
constexpr std::uint32_t kMessages = 1'000'000;
constexpr const char* kShmName = "/pulsegrid_demo";

std::uint64_t now_ns() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<
            std::chrono::nanoseconds
        >(
            std::chrono::steady_clock::now()
                .time_since_epoch()
        ).count()
    );
}

} // namespace

int main() {
    using Queue =
        pulsegrid::ShmQueue<kCapacity>;

    try {
        pulsegrid::ShmRegion region{
            kShmName,
            Queue::mapped_size(),
            pulsegrid::ShmRegion::Mode::Create
        };

        auto queue =
            Queue::initialize(region.data());

        std::cout
            << "PulseGrid producer ready\n"
            << "Publishing "
            << kMessages
            << " updates\n";

        for (std::uint32_t sequence = 0;
             sequence < kMessages;
             ++sequence) {

            pulsegrid::Update update{
                .table_id = 1,
                .row_id = sequence % 1000,
                .column_id = 1,
                .type = pulsegrid::ValueType::UInt64,
                .sequence = sequence,
                .timestamp_ns = now_ns(),
                .payload = sequence
            };

            while (!queue.try_push(update)) {
                std::this_thread::yield();
            }
        }

        std::cout
            << "Published all updates.\n"
            << "Waiting for consumer completion...\n";

        while (!queue.consumer_complete()) {
            std::this_thread::yield();
        }

        region.unlink();

        std::cout << "Producer complete\n";
    }
    catch (const std::exception& error) {
        std::cerr
            << "producer error: "
            << error.what()
            << '\n';

        return 1;
    }

    return 0;
}
