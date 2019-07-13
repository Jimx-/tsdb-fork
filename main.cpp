#include "db/DB.hpp"
#include "db/DBAdapter.hpp"
#include "promql/web/http_server.h"
#include "tagtree/adapters/prom/indexed_storage.h"

int main()
{
    tsdb::db::DB db(tmpnam(nullptr));
    tsdb::db::DBAdapter adapter(&db);
    tagtree::prom::IndexedStorage indexed_storage(&adapter);
    promql::HttpServer server(&indexed_storage);

    server.start();

    return 0;
}
