#include "db/DB.hpp"
#include "db/DBAdapter.hpp"
#include "promql/web/http_server.h"
#include "tagtree/adapters/prom/indexed_storage.h"
#include "tagtree/series/btree_series_manager.h"

int main()
{
    tsdb::db::DB db(tmpnam(nullptr));
    tagtree::BTreeSeriesManager bsm(4096, "series.db", "series.index");
    tsdb::db::DBAdapter adapter(&db);
    tagtree::prom::IndexedStorage indexed_storage("index.db", 4096, &adapter,
                                                  &bsm);
    promql::HttpServer prom_server(&indexed_storage);

    prom_server.start();

    return 0;
}
