#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include "benchmark_common.hpp"
#include "pulsegrid/update.hpp"

namespace {

constexpr std::uint64_t kWarmupSamples = 10'000;
constexpr std::uint64_t kMeasuredSamples = 1'000'000;

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
            sent +=
                static_cast<std::size_t>(result);
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

} // namespace

int main() {
    int sockets[2];

    if (::socketpair(
            AF_UNIX,
            SOCK_STREAM,
            0,
            sockets
        ) == -1) {

        std::cerr
            << "socketpair: "
            << std::strerror(errno)
            << '\n';

        return 1;
    }

    const pid_t child = ::fork();

    if (child == -1) {
        ::close(sockets[0]);
        ::close(sockets[1]);

        std::cerr << "fork failed\n";

        return 1;
    }

    if (child == 0) {
        ::close(sockets[0]);

        try {
            const std::uint64_t total =
                kWarmupSamples +
                kMeasuredSamples;

            for (std::uint64_t i = 1;
                 i <= total;
                 ++i) {

                pulsegrid::Update update{};

                receive_all(
                    sockets[1],
                    &update,
                    sizeof(update)
                );

                if (update.sequence !=
                        static_cast<std::uint32_t>(i) ||
                    update.payload != i) {

                    std::_Exit(2);
                }

                const std::uint64_t ack = i;

                send_all(
                    sockets[1],
                    &ack,
                    sizeof(ack)
                );
            }

            ::close(sockets[1]);

            std::_Exit(0);
        }
        catch (...) {
            std::_Exit(3);
        }
    }

    ::close(sockets[1]);

    try {
        // Warm-up.
        for (std::uint64_t i = 1;
             i <= kWarmupSamples;
             ++i) {

            const auto update =
                make_update(
                    static_cast<std::uint32_t>(i)
                );

            send_all(
                sockets[0],
                &update,
                sizeof(update)
            );

            std::uint64_t ack = 0;

            receive_all(
                sockets[0],
                &ack,
                sizeof(ack)
            );

            if (ack != i) {
                throw std::runtime_error(
                    "warmup acknowledgement mismatch"
                );
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

            send_all(
                sockets[0],
                &update,
                sizeof(update)
            );

            std::uint64_t ack = 0;

            receive_all(
                sockets[0],
                &ack,
                sizeof(ack)
            );

            const auto end =
                pulsegrid::bench::Clock::now();

            if (ack != sequence) {
                throw std::runtime_error(
                    "acknowledgement mismatch"
                );
            }

            const auto ns =
                std::chrono::duration_cast<
                    std::chrono::nanoseconds
                >(end - start).count();

            samples.push_back(
                static_cast<std::uint64_t>(ns)
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
            "unix_socket",
            summary
        );
    }
    catch (const std::exception& error) {
        ::close(sockets[0]);

        std::cerr
            << "benchmark error: "
            << error.what()
            << '\n';

        return 1;
    }

    return 0;
}
