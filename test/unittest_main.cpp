#include <gtest/gtest.h>
// #include <google/profiler.h>

void db_bench();
void xorchunk_bench();

int main(int argc, char *argv[]){
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::GTEST_FLAG(filter) = "DBTest*";
    // db_bench();
    return RUN_ALL_TESTS();
}