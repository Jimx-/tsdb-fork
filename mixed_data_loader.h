#ifndef _ZIPF_DATA_LOADER_H_
#define _ZIPF_DATA_LOADER_H_

#include "base_data_loader.h"

#include "promql/labels.h"
#include "tagtree/adapters/prom/indexed_storage.h"

#include <atomic>
#include <string>

class MixedDataLoader : public BaseDataLoader {
public:
    MixedDataLoader(std::string_view filename, double alpha, double rw_ratio);

    void load(tagtree::prom::IndexedStorage& storage, int num_workers = 8);

private:
    bool stopped;
    double alpha;
    double rw_ratio;

    std::atomic<size_t> inserted_rows;
    std::atomic<size_t> queried_rows;
};

#endif
