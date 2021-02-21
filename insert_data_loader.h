#ifndef _INSERT_DATA_LOADER_H_
#define _INSERT_DATA_LOADER_H_

#include "base_data_loader.h"

#include "promql/labels.h"
#include "tagtree/adapters/prom/indexed_storage.h"

#include <atomic>
#include <string>

class InsertDataLoader : public BaseDataLoader {
public:
    InsertDataLoader(std::string_view filename);

    void load(tagtree::prom::IndexedStorage& storage, int num_workers = 8);

private:
    std::atomic<int> stopped;
    std::atomic<size_t> rowidx;
    std::atomic<size_t> inserted_row;
};

#endif
