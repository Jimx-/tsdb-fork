#ifndef TESTUTILS_H
#define TESTUTILS_H

#include "base/Logging.hpp"

#include <deque>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>
#include <string>

#define PRINTF(...)  do { testing::internal::ColoredPrintf(testing::internal::COLOR_GREEN, "[          ] "); testing::internal::ColoredPrintf(testing::internal::COLOR_YELLOW, __VA_ARGS__); } while(0)

// C++ stream interface
class TestCout : public std::stringstream
{
public:
    ~TestCout()
    {
        PRINTF("%s",str().c_str());
    }
};

#define TEST_COUT  TestCout()

namespace tsdb{
namespace test{

// void print_start(const std::string & name){
//     std::cout << "----------------------- " << name << " -----------------------" << std::endl;
// }

// void print_end(const std::string & name){
//     std::cout << "------------------------------------------------";
//     for(int i = 0; i < name.length(); i ++)
//         std::cout << "-";
//     std::cout << std::endl << std::endl;
// }

class Header{
    public:
        std::string name;
        Header(const std::string & name): name(name){
            std::cout << "----------------------- " << name << " -----------------------" << std::endl;
        }

        ~Header(){
            std::cout << "------------------------------------------------";
            for(int i = 0; i < name.length(); i ++)
                std::cout << "-";
            std::cout << std::endl << std::endl;
        }
};

std::deque<std::deque<double>> load_sample_data(const std::string & filename);

}}

#endif