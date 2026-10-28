#define _AMPS_SKIP_AMPSPLUSPLUS
#include <amps/amps.h>
#define _AMPSPLUSPLUS_H_
#include <amps/ampscrc.hpp>
#include <amps/CompositeMessageBuilder.hpp>
#include <iostream>

int main() {
    AMPS::CompositeMessageBuilder builder;
    builder.append(std::string(""));
    std::cout << "empty_part_bytes=" << builder.length() << '\n';
    std::cout << "crc_123456789=" << AMPS::CRC<0>::crcNoSSE("123456789", 9) << '\n';
    std::cout << "crc_crc_data=" << AMPS::CRC<0>::crcNoSSE("crc-data", 8) << '\n';
}
