#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>

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

bool receive_all(
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
            return false;
        }

        if (errno == EINTR) {
            continue;
        }

        throw std::runtime_error(
            std::string("recv: ") +
            std::strerror(errno)
        );
    }

    return true;
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
        std::cerr << "fork failed\n";

        ::close(sockets[0]);
        ::close(sockets[1]);

        return 1;
    }

    if (child == 0) {
        ::close(sockets[0]);

        try {
            const std::uint64_t total =
                pulsegrid::bench::kWarmupMessages +
                pulsegrid::bench::kMeasuredMessages;

            for (std::uint64_t i = 0;
                 i < total;
                 ++i) {

                pulsegrid::Update update{};

                if (!receive_all(
                        sockets[1],
                        &update,
                        sizeof(update)
                    )) {
                    std::_Exit(4);
                }

                const auto expected =
                    static_cast<std::uint32_t>(i);

                if (update.sequence != expected ||
                    update.payload != expected) {
                    std::_Exit(2);
                }
            }

            const std::uint8_t done = 1;

            send_all(
                sockets[1],
                &done,
                sizeof(done)
            );

            ::close(sockets[1]);

            std::_Exit(0);
        }
        catch (...) {
            std::_Exit(3);
        }
    }

    ::close(sockets[1]);

    try {
        for (std::uint64_t i = 0;
             i < pulsegrid::bench::kWarmupMessages;
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

            send_all(
                sockets[0],
                &update,
                sizeof(update)
            );
        }

        // Wait for the consumer to confirm that all
        // measured messages were actually received.
        std::uint8_t done = 0;

        if (!receive_all(
                sockets[0],
                &done,
                sizeof(done)
            ) ||
            done != 1) {

            throw std::runtime_error(
                "consumer completion handshake failed"
            );
        }

        const auto end =
            pulsegrid::bench::Clock::now();

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

        pulsegrid::bench::print_throughput(
            "unix_socket",
            result
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
