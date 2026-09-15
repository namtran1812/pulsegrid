#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include "benchmark_common.hpp"
#include "pulsegrid/update.hpp"

namespace {

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

void send_all(
    int fd,
    const void* data,
    std::size_t size
) {
    const auto* bytes =
        static_cast<const std::byte*>(data);

    std::size_t sent = 0;

    while (sent < size) {
        const ssize_t result =
            ::send(
                fd,
                bytes + sent,
                size - sent,
                0
            );

        if (result > 0) {
            sent += static_cast<std::size_t>(result);
            continue;
        }

        if (result == -1 && errno == EINTR) {
            continue;
        }

        throw std::runtime_error(
            std::string("send: ") +
            std::strerror(errno)
        );
    }
}

void receive_all(
    int fd,
    void* data,
    std::size_t size
) {
    auto* bytes =
        static_cast<std::byte*>(data);

    std::size_t received = 0;

    while (received < size) {
        const ssize_t result =
            ::recv(
                fd,
                bytes + received,
                size - received,
                0
            );

        if (result > 0) {
            received +=
                static_cast<std::size_t>(result);
            continue;
        }

        if (result == 0) {
            throw std::runtime_error(
                "unexpected socket EOF"
            );
        }

        if (errno == EINTR) {
            continue;
        }

        throw std::runtime_error(
            std::string("recv: ") +
            std::strerror(errno)
        );
    }
}

std::size_t parse_batch_size(
    int argc,
    char** argv
) {
    if (argc != 2) {
        throw std::invalid_argument(
            "usage: pulsegrid_uds_batched_throughput "
            "<batch_size>"
        );
    }

    const auto value =
        std::stoull(argv[1]);

    if (value == 0) {
        throw std::invalid_argument(
            "batch size must be positive"
        );
    }

    return static_cast<std::size_t>(value);
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

        int sockets[2];

        if (::socketpair(
                AF_UNIX,
                SOCK_STREAM,
                0,
                sockets
            ) == -1) {

            throw std::runtime_error(
                std::string("socketpair: ") +
                std::strerror(errno)
            );
        }

        const pid_t child = ::fork();

        if (child == -1) {
            ::close(sockets[0]);
            ::close(sockets[1]);

            throw std::runtime_error(
                "fork failed"
            );
        }

        if (child == 0) {
            ::close(sockets[0]);

            try {
                std::vector<pulsegrid::Update> batch(
                    batch_size
                );

                // Phase 1: warm-up.
                std::uint64_t consumed = 0;

                while (
                    consumed <
                    pulsegrid::bench::kWarmupMessages
                ) {
                    receive_all(
                        sockets[1],
                        batch.data(),
                        batch.size() *
                            sizeof(pulsegrid::Update)
                    );

                    for (std::size_t j = 0;
                         j < batch.size();
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

                    consumed += batch.size();
                }

                // Explicit warm-up completion handshake.
                const std::uint8_t warmup_done = 1;

                send_all(
                    sockets[1],
                    &warmup_done,
                    sizeof(warmup_done)
                );

                // Phase 2: measured messages.
                const auto total =
                    pulsegrid::bench::kWarmupMessages +
                    pulsegrid::bench::kMeasuredMessages;

                while (consumed < total) {
                    receive_all(
                        sockets[1],
                        batch.data(),
                        batch.size() *
                            sizeof(pulsegrid::Update)
                    );

                    for (std::size_t j = 0;
                         j < batch.size();
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

                    consumed += batch.size();
                }

                const std::uint8_t measured_done = 1;

                send_all(
                    sockets[1],
                    &measured_done,
                    sizeof(measured_done)
                );

                ::close(sockets[1]);
                std::_Exit(0);
            }
            catch (...) {
                std::_Exit(3);
            }
        }

        ::close(sockets[1]);

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

                    send_all(
                        sockets[0],
                        batch.data(),
                        batch.size() *
                            sizeof(pulsegrid::Update)
                    );
                }
            };

        publish_range(
            0,
            pulsegrid::bench::kWarmupMessages
        );

        std::uint8_t warmup_done = 0;

        receive_all(
            sockets[0],
            &warmup_done,
            sizeof(warmup_done)
        );

        if (warmup_done != 1) {
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

        std::uint8_t measured_done = 0;

        receive_all(
            sockets[0],
            &measured_done,
            sizeof(measured_done)
        );

        const auto end =
            pulsegrid::bench::Clock::now();

        if (measured_done != 1) {
            throw std::runtime_error(
                "measurement handshake failed"
            );
        }

        ::close(sockets[0]);

        int status = 0;

        if (::waitpid(child, &status, 0) != child) {
            throw std::runtime_error(
                "waitpid failed"
            );
        }

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
            "unix_socket_batched",
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
