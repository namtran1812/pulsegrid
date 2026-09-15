#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

namespace pulsegrid::bench {

using Clock = std::chrono::steady_clock;

inline constexpr std::uint64_t kWarmupMessages = 131'072;
inline constexpr std::uint64_t kMeasuredMessages = 8'388'608;

inline std::uint64_t now_ns() noexcept {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<
            std::chrono::nanoseconds
        >(
            Clock::now().time_since_epoch()
        ).count()
    );
}

struct ThroughputResult {
    std::uint64_t messages{};
    double seconds{};
    double messages_per_second{};
    double gib_per_second{};
};

inline ThroughputResult make_throughput_result(
    std::uint64_t messages,
    std::size_t bytes_per_message,
    Clock::duration elapsed
) {
    const double seconds =
        std::chrono::duration<double>(
            elapsed
        ).count();

    if (seconds <= 0.0) {
        throw std::runtime_error(
            "invalid benchmark duration"
        );
    }

    const double messages_per_second =
        static_cast<double>(messages) / seconds;

    const double bytes_per_second =
        messages_per_second *
        static_cast<double>(bytes_per_message);

    constexpr double kGiB =
        1024.0 * 1024.0 * 1024.0;

    return {
        messages,
        seconds,
        messages_per_second,
        bytes_per_second / kGiB
    };
}

inline void print_throughput(
    const std::string& transport,
    const ThroughputResult& result
) {
    std::cout
        << std::fixed
        << std::setprecision(3)
        << "transport=" << transport << '\n'
        << "messages=" << result.messages << '\n'
        << "seconds=" << result.seconds << '\n'
        << "messages_per_second="
        << result.messages_per_second << '\n'
        << "gib_per_second="
        << result.gib_per_second << '\n';
}

struct LatencySummary {
    double mean_ns{};
    std::uint64_t p50_ns{};
    std::uint64_t p95_ns{};
    std::uint64_t p99_ns{};
    std::uint64_t p999_ns{};
    std::uint64_t max_ns{};
};

inline std::uint64_t percentile(
    const std::vector<std::uint64_t>& sorted,
    double p
) {
    if (sorted.empty()) {
        throw std::invalid_argument(
            "cannot calculate percentile of empty data"
        );
    }

    const double position =
        p * static_cast<double>(sorted.size() - 1);

    const auto index =
        static_cast<std::size_t>(
            std::ceil(position)
        );

    return sorted[index];
}

inline LatencySummary summarize_latency(
    std::vector<std::uint64_t> samples
) {
    if (samples.empty()) {
        throw std::invalid_argument(
            "no latency samples"
        );
    }

    const auto total =
        std::accumulate(
            samples.begin(),
            samples.end(),
            static_cast<long double>(0.0)
        );

    std::sort(
        samples.begin(),
        samples.end()
    );

    return {
        .mean_ns =
            static_cast<double>(
                total /
                static_cast<long double>(
                    samples.size()
                )
            ),
        .p50_ns = percentile(samples, 0.50),
        .p95_ns = percentile(samples, 0.95),
        .p99_ns = percentile(samples, 0.99),
        .p999_ns = percentile(samples, 0.999),
        .max_ns = samples.back()
    };
}

inline void print_latency(
    const std::string& transport,
    const LatencySummary& summary
) {
    std::cout
        << std::fixed
        << std::setprecision(2)
        << "transport=" << transport << '\n'
        << "mean_ns=" << summary.mean_ns << '\n'
        << "p50_ns=" << summary.p50_ns << '\n'
        << "p95_ns=" << summary.p95_ns << '\n'
        << "p99_ns=" << summary.p99_ns << '\n'
        << "p999_ns=" << summary.p999_ns << '\n'
        << "max_ns=" << summary.max_ns << '\n';
}

} // namespace pulsegrid::bench
