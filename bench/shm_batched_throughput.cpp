#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
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

constexpr std::size_t kCapacity = 65'536;

using Queue =
    pulsegrid::ShmQueue<kCapacity>;

std::string unique_shm_name() {
    return "/pulsegrid_batch_bench_" +
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

std::size_t parse_batch_size(
    int argc,
    char** argv
) {
    if (argc != 2) {
        throw std::invalid_argument(
            "usage: pulsegrid_shm_batched_throughput "
            "<batch_size>"
        );
    }

    const auto value =
        std::stoull(argv[1]);

    if (value == 0 || value > kCapacity) {
        throw std::invalid_argument(
            "batch size must be in [1, 65536]"
        );
    }

    return static_cast<std::size_t>(value);
}

void write_signal(
    int fd,
    std::uint8_t signal
) {
    while (true) {
        const auto result =
            ::write(
                fd,
                &signal,
                sizeof(signal)
            );

        if (result ==
            static_cast<ssize_t>(
                sizeof(signal)
            )) {
            return;
        }

        if (result == -1 &&
            errno == EINTR) {
            continue;
        }

        throw std::runtime_error(
            std::string("control write: ") +
            std::strerror(errno)
        );
    }
}

std::uint8_t read_signal(
    int fd
) {
    std::uint8_t signal = 0;

    while (true) {
        const auto result =
            ::read(
                fd,
                &signal,
                sizeof(signal)
            );

        if (result ==
            static_cast<ssize_t>(
                sizeof(signal)
            )) {
            return signal;
        }

        if (result == -1 &&
            errno == EINTR) {
            continue;
        }

        if (result == 0) {
            throw std::runtime_error(
                "control pipe closed unexpectedly"
            );
        }

        throw std::runtime_error(
            std::string("control read: ") +
            std::strerror(errno)
        );
    }
}

} // namespace

int main(int argc, char** argv) {
    try {
        const std::size_t batch_size =
            parse_batch_size(argc, argv);

        if (pulsegrid::bench::kWarmupMessages %
                batch_size != 0 ||
            pulsegrid::bench::kMeasuredMessages %
                batch_size != 0) {

            throw std::invalid_argument(
                "message counts must be divisible "
                "by batch size"
            );
        }

        const std::string name =
            unique_shm_name();

        pulsegrid::ShmRegion region{
            name,
            Queue::mapped_size(),
            pulsegrid::ShmRegion::Mode::Create
        };

        auto producer =
            Queue::initialize(region.data());

        // Benchmark-only control plane.
        // Shared memory remains the measured data plane.
        int control_pipe[2];

        if (::pipe(control_pipe) == -1) {
            region.unlink();

            throw std::runtime_error(
                std::string("pipe: ") +
                std::strerror(errno)
            );
        }

        const pid_t child = ::fork();

        if (child == -1) {
            ::close(control_pipe[0]);
            ::close(control_pipe[1]);
            region.unlink();

            throw std::runtime_error(
                "fork failed"
            );
        }

        if (child == 0) {
            ::close(control_pipe[0]);

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

                std::vector<pulsegrid::Update> batch(
                    batch_size
                );

                std::uint64_t consumed = 0;

                // Phase 1: warm-up.
                while (
                    consumed <
                    pulsegrid::bench::kWarmupMessages
                ) {
                    const auto remaining =
                        pulsegrid::bench::kWarmupMessages -
                        consumed;

                    const auto requested =
                        std::min<std::size_t>(
                            batch.size(),
                            static_cast<std::size_t>(
                                remaining
                            )
                        );

                    const auto count =
                        consumer.try_pop_batch(
                            batch.data(),
                            requested
                        );

                    if (count == 0) {
                        std::this_thread::yield();
                        continue;
                    }

                    for (std::size_t j = 0;
                         j < count;
                         ++j) {

                        const auto expected64 =
                            consumed + j;

                        if (
                            batch[j].sequence !=
                                static_cast<std::uint32_t>(
                                    expected64
                                ) ||
                            batch[j].payload != expected64
                        ) {
                            std::_Exit(2);
                        }
                    }

                    consumed += count;
                }

                // Tell parent that every warm-up message
                // has been consumed.
                write_signal(
                    control_pipe[1],
                    1
                );

                // Phase 2: measured workload.
                const auto total =
                    pulsegrid::bench::kWarmupMessages +
                    pulsegrid::bench::kMeasuredMessages;

                while (consumed < total) {
                    const auto remaining =
                        total - consumed;

                    const auto requested =
                        std::min<std::size_t>(
                            batch.size(),
                            static_cast<std::size_t>(
                                remaining
                            )
                        );

                    const auto count =
                        consumer.try_pop_batch(
                            batch.data(),
                            requested
                        );

                    if (count == 0) {
                        std::this_thread::yield();
                        continue;
                    }

                    for (std::size_t j = 0;
                         j < count;
                         ++j) {

                        const auto expected64 =
                            consumed + j;

                        if (
                            batch[j].sequence !=
                                static_cast<std::uint32_t>(
                                    expected64
                                ) ||
                            batch[j].payload != expected64
                        ) {
                            std::_Exit(2);
                        }
                    }

                    consumed += count;
                }

                // Tell parent that all measured messages
                // have actually been consumed.
                write_signal(
                    control_pipe[1],
                    2
                );

                ::close(control_pipe[1]);

                std::_Exit(0);
            }
            catch (...) {
                ::close(control_pipe[1]);
                std::_Exit(3);
            }
        }

        // Parent only reads benchmark control signals.
        ::close(control_pipe[1]);

        std::vector<pulsegrid::Update> batch(
            batch_size
        );

        auto publish_range =
            [&](std::uint64_t begin,
                std::uint64_t count) {

                for (std::uint64_t offset = 0;
                     offset < count;
                     offset += batch_size) {

                    for (std::size_t j = 0;
                         j < batch_size;
                         ++j) {

                        const auto sequence =
                            begin + offset + j;

                        batch[j] =
                            make_update(
                                static_cast<std::uint32_t>(
                                    sequence
                                )
                            );
                    }

                    while (
                        !producer.try_push_batch(
                            batch.data(),
                            batch.size()
                        )
                    ) {
                        std::this_thread::yield();
                    }
                }
            };

        // Warm-up is outside the measured interval.
        publish_range(
            0,
            pulsegrid::bench::kWarmupMessages
        );

        const auto warmup_signal =
            read_signal(control_pipe[0]);

        if (warmup_signal != 1) {
            throw std::runtime_error(
                "warm-up handshake failed"
            );
        }

        const auto start =
            pulsegrid::bench::Clock::now();

        publish_range(
            pulsegrid::bench::kWarmupMessages,
            pulsegrid::bench::kMeasuredMessages
        );

        // Timer ends only after the consumer has drained
        // the entire measured workload.
        const auto measured_signal =
            read_signal(control_pipe[0]);

        const auto end =
            pulsegrid::bench::Clock::now();

        if (measured_signal != 2) {
            throw std::runtime_error(
                "measurement handshake failed"
            );
        }

        ::close(control_pipe[0]);

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

        std::cout
            << "batch_size="
            << batch_size
            << '\n';

        pulsegrid::bench::print_throughput(
            "shm_batched",
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
