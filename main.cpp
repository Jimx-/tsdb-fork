#include "bptree/mem_page_cache.h"
#include "null_storage.h"
#include "promql/web/http_server.h"
#include "tagtree/adapters/prom/indexed_storage.h"
#include "tagtree/series/series_file_manager.h"
#include "tagtree/tree/cow_tree_node.h"

#include <signal.h>

static void sigint_handler(int signum) { exit(130); }

int main()
{
    NullStorage null_storage;
    tagtree::SeriesFileManager sfm(200000, "./series", 50000);
    tagtree::prom::IndexedStorage indexed_storage(".", 4096, &null_storage,
                                                  &sfm);
    promql::HttpServer prom_server(&indexed_storage, 8);

    struct sigaction sa;
    sa.sa_flags = 0;
    sa.sa_handler = sigint_handler;
    sigaction(SIGINT, &sa, nullptr);

    prom_server.start();

    return 0;
}
