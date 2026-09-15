#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <sys/wait.h>
#include <unistd.h>

#include "pulsegrid/shm_queue.hpp"
#include "pulsegrid/shm_region.hpp"

namespace {

constexpr std::size_t kCapacity = 1024;
constexpr std::uint32_t kMessages = 1'000'000;

std::string unique_shm_name() {
    return "/pulsegrid_process_test_" +
           std::to_string(
               static_cast<long long>(::getpid())
           );
}

} // namespace

TEST(ShmQueueProcess, TransfersOrderedUpdatesAcrossProcesses) {
    using Queue = pulsegrid::ShmQueue<kCapacity>;

    const std::string name = unique_shm_name();

    pulsegrid::ShmRegion region{
        name,
        Queue::mapped_size(),
        pulsegrid::ShmRegion::Mode::Create
    };

    auto producer_queue =
        Queue::initialize(region.data());

    const pid_t child = ::fork();

    ASSERT_NE(child, -1);

    if (child == 0) {
        try {
            pulsegrid::ShmRegion child_region{
                name,
                Queue::mapped_size(),
                pulsegrid::ShmRegion::Mode::Open
            };

            auto consumer_queue =
                Queue::attach(child_region.data());

            consumer_queue.mark_consumer_attached();

            for (std::uint32_t expected = 0;
                 expected < kMessages;
                 ++expected) {

                pulsegrid::Update update{};

                while (!consumer_queue.try_pop(update)) {
                    std::this_thread::yield();
                }

                if (update.sequence != expected ||
                    update.payload != expected) {
                    std::_Exit(2);
                }
            }

            consumer_queue.mark_consumer_complete();

            std::_Exit(0);
        }
        catch (...) {
            std::_Exit(3);
        }
    }

    for (std::uint64_t sequence = 0;
         sequence < kMessages;
         ++sequence) {

        pulsegrid::Update update{
            .table_id = 1,
            .row_id = static_cast<std::uint32_t>(sequence % 1000),
            .column_id = 1,
            .type = pulsegrid::ValueType::UInt64,
            .sequence = sequence,
            .payload = sequence
        };

        while (!producer_queue.try_push(update)) {
            std::this_thread::yield();
        }
    }

    int status = 0;

    ASSERT_EQ(
        ::waitpid(child, &status, 0),
        child
    );

    EXPECT_TRUE(WIFEXITED(status));

    if (WIFEXITED(status)) {
        EXPECT_EQ(WEXITSTATUS(status), 0);
    }

    EXPECT_TRUE(
        producer_queue.consumer_complete()
    );

    region.unlink();
}
