#include <iostream>

#include "pulsegrid/update.hpp"

int main() {
    pulsegrid::Update update{
        .table_id = 1,
        .row_id = 42,
        .column_id = 3,
        .type = pulsegrid::ValueType::Double,
        .sequence = 1,
        .timestamp_ns = 0,
        .payload = 0
    };

    std::cout << "PulseGrid producer\n";
    std::cout << "Update size: " << sizeof(update) << " bytes\n";

    return 0;
}
