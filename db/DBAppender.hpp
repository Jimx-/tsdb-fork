#ifndef DBAPPENDER_H
#define DBAPPENDER_H

#include "db/AppenderInterface.hpp"
#include "db/DB.hpp"

namespace tsdb {
namespace db {

// DBAppender wraps the DB's head appender and triggers compactions on commit
// if necessary.
class DBAppender : public AppenderInterface {
private:
    std::unique_ptr<db::AppenderInterface> app;
    db::DB* db;

public:
    DBAppender(std::unique_ptr<db::AppenderInterface>&& app, db::DB* db)
        : app(std::move(app)), db(db)
    {}

    error::Error add(const tagtree::TSID& tsid, int64_t t, double v)
    {
        return app->add(tsid, t, v);
    }

    error::Error commit()
    {
        error::Error err = app->commit();
        // We could just run this check every few minutes practically. But for
        // benchmarks and high frequency use cases this is the safer way.
        if (db->head()->MaxTime() - db->head()->MinTime() >
            db->head()->chunk_range / 2 * 3) {
            db->compact_channel()->send(0);
            // LOG_DEBUG << db->head()->MinTime() << " " <<
            // db->head()->MaxTime() << " " << (db->head()->MaxTime() -
            // db->head()->MinTime() > db->head()->chunk_range / 2 * 3);
        }

        return err;
    }

    error::Error rollback() { return app->rollback(); }

    ~DBAppender()
    {
        // LOG_DEBUG << "~DBAppender";
    }
};

} // namespace db
} // namespace tsdb

#endif
