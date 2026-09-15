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

} // namespace

int main() {
    using Queue =
        pulsegrid::ShmQueue<kCapacity>;

    try {
        pulsegrid::ShmRegion region{
            kShmName,
            Queue::mapped_size(),
            pulsegrid::ShmRegion::Mode::Open
        };

        auto queue =
            Queue::attach(region.data());

        queue.mark_consumer_attached();

        std::cout << "PulseGrid consumer attached\n";

        const auto start =
            std::chrono::steady_clock::now();

        for (std::uint32_t expected = 0;
             expected < kMessages;
             ++expected) {

            pulsegrid::Update update{};

            while (!queue.try_pop(update)) {
                std::this_thread::yield();
            }

            if (update.sequence != expected) {
                std::cerr
                    << "sequence violation: expected "
                    << expected
                    << ", received "
                    << update.sequence
                    << '\n';

                return 2;
            }

            if (update.payload != expected) {
                std::cerr
                    << "payload corruption at sequence "
                    << expected
                    << '\n';

                return 3;
            }
        }

        const auto end =
            std::chrono::steady_clock::now();

        const auto elapsed =
            std::chrono::duration<double>(
                end - start
            ).count();

        const double rate =
            static_cast<double>(kMessages)
            / elapsed;

        queue.mark_consumer_complete();

        std::cout
            << "Consumed "
            << kMessages
            << " updates\n"
            << "Elapsed: "
            << elapsed
            << " s\n"
            << "Rate: "
            << rate
            << " updates/s\n";
    }
    catch (const std::exception& error) {
        std::cerr
            << "consumer error: "
            << error.what()
            << '\n';

        return 1;
    }

    return 0;
}
