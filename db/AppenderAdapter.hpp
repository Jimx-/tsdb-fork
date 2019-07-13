#ifndef APPENDERADAPTER_H
#define APPENDERADAPTER_H

#include "db/AppenderInterface.hpp"
#include "db/DB.hpp"
#include "tagtree/storage.h"

namespace tsdb {
namespace db {

class AppenderAdapter : public tagtree::Appender {
public:
    AppenderAdapter(std::shared_ptr<AppenderInterface> app) : app(app) {}

    virtual void add(const tagtree::TSID& tsid, uint64_t t, double v)
    {
        app->add(tsid, t, v);
    }

    virtual void commit() { app->commit(); }

private:
    std::shared_ptr<AppenderInterface> app;
};

} // namespace db
} // namespace tsdb

#endif
