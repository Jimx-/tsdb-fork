#include "db/DB.hpp"
#include "db/DBAdapter.hpp"
#include "promql/web/http_server.h"
#include "tagtree/adapters/prom/indexed_storage.h"
#include "tagtree/series/series_file_manager.h"

int main()
{
    tsdb::db::DB db(tmpnam(nullptr));
    tagtree::SeriesFileManager sfm(200000, "./series", 50000);
    tsdb::db::DBAdapter adapter(&db);
    tagtree::prom::IndexedStorage indexed_storage(".", 4096, &adapter, &sfm);
    promql::HttpServer prom_server(&indexed_storage);

    prom_server.start();

    return 0;
}
