#ifndef DBADAPTER_H
#define DBADAPTER_H

#include "db/AppenderAdapter.hpp"
#include "db/DB.hpp"
#include "querier/QuerierAdapter.hpp"
#include "tagtree/storage.h"

namespace tsdb {
namespace db {

class DBAdapter : public tagtree::Storage {
public:
    DBAdapter(DB* db) : db(db) {}

    virtual std::shared_ptr<tagtree::Querier> querier(uint64_t mint,
                                                      uint64_t maxt)
    {
        auto q = db->querier(mint, maxt);
        return std::make_shared<querier::QuerierAdapter>(std::move(q.first));
    }

    virtual std::shared_ptr<tagtree::Appender> appender()
    {
        return std::make_shared<AppenderAdapter>(db->appender());
    }

private:
    DB* db;
};

} // namespace db
} // namespace tsdb

#endif
