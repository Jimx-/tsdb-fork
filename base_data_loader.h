#ifndef _BASE_DATA_LOADER_H_
#define _BASE_DATA_LOADER_H_

#include "promql/labels.h"

#include <atomic>
#include <string>

class BaseDataLoader {
public:
    BaseDataLoader(std::string_view filename);

protected:
    std::string filename;
    std::vector<std::string> dataset;

    void load_file();
};

#endif
