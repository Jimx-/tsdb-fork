#include "base_data_loader.h"

#include <fstream>

BaseDataLoader::BaseDataLoader(std::string_view filename) : filename(filename)
{}

void BaseDataLoader::load_file()
{
    std::ifstream fin(filename);

    if (!fin.is_open()) {
        throw std::runtime_error("file not found");
    }

    std::string line;
    while (!fin.eof()) {
        std::getline(fin, line);
        if (line.empty()) break;

        dataset.emplace_back(std::move(line));
    }
}
