#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

#include <sys/wait.h>
#include <unistd.h>

#include "benchmark_common.hpp"
#include "pulsegrid/shm_queue.hpp"
#include "pulsegrid/shm_region.hpp"

namespace {

constexpr std::size_t kCapacity = 65'536;

std::string unique_shm_name() {
    return "/pulsegrid_bench_" +
           std::to_string(
               static_cast<long long>(::getpid())
           );
}

pulsegrid::Update make_update(
    std::uint64_t sequence
) {
    return {
        .table_id = 1,
        .row_id = static_cast<std::uint32_t>(sequence % 1000),
        .column_id = 1,
        .type = pulsegrid::ValueType::UInt64,
        .sequence = sequence,
        .payload = sequence
    };
}

} // namespace

int main() {
    using Queue =
        pulsegrid::ShmQueue<kCapacity>;

    const std::string name =
        unique_shm_name();

    try {
        pulsegrid::ShmRegion region{
            name,
            Queue::mapped_size(),
            pulsegrid::ShmRegion::Mode::Create
        };

        auto producer =
            Queue::initialize(region.data());

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
                    Queue::mapped_size(),
                    pulsegrid::ShmRegion::Mode::Open
                };

                auto consumer =
                    Queue::attach(
                        child_region.data()
                    );

                consumer.mark_consumer_attached();

                const std::uint64_t total =
                    pulsegrid::bench::kWarmupMessages +
                    pulsegrid::bench::kMeasuredMessages;

                for (std::uint64_t i = 0;
                     i < total;
                     ++i) {

                    pulsegrid::Update update{};

                    while (!consumer.try_pop(update)) {
                        std::this_thread::yield();
                    }

                    const auto expected =
                        static_cast<std::uint32_t>(i);

                    if (update.sequence != expected ||
                        update.payload != expected) {
                        std::_Exit(2);
                    }
                }

                consumer.mark_consumer_complete();

                std::_Exit(0);
            }
            catch (...) {
                std::_Exit(3);
            }
        }

        // Warm-up phase. These messages are deliberately
        // excluded from the measured interval.
        for (std::uint64_t i = 0;
             i < pulsegrid::bench::kWarmupMessages;
             ++i) {

            const auto sequence =
                static_cast<std::uint32_t>(i);

            const auto update =
                make_update(sequence);

            while (!producer.try_push(update)) {
                std::this_thread::yield();
            }
        }

        const auto start =
            pulsegrid::bench::Clock::now();

        for (std::uint64_t i = 0;
             i < pulsegrid::bench::kMeasuredMessages;
             ++i) {

            const auto sequence =
                static_cast<std::uint32_t>(
                    pulsegrid::bench::kWarmupMessages + i
                );

            const auto update =
                make_update(sequence);

            while (!producer.try_push(update)) {
                std::this_thread::yield();
            }
        }

        // Producer enqueue completion is not enough.
        // End-to-end throughput includes consumer drain.
        while (!producer.consumer_complete()) {
            std::this_thread::yield();
        }

        const auto end =
            pulsegrid::bench::Clock::now();

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

        const auto result =
            pulsegrid::bench::make_throughput_result(
                pulsegrid::bench::kMeasuredMessages,
                sizeof(pulsegrid::Update),
                end - start
            );

        pulsegrid::bench::print_throughput(
            "shm",
            result
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
