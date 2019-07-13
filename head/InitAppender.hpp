#ifndef INITAPPENDER_H
#define INITAPPENDER_H

#include "db/AppenderInterface.hpp"
#include "head/Head.hpp"
#include "head/HeadUtils.hpp"

namespace tsdb {
namespace head {

class InitAppender : public db::AppenderInterface {
private:
    std::unique_ptr<db::AppenderInterface> app;
    Head* head;

public:
    InitAppender(Head* head) : head(head) {}

    error::Error add(const tagtree::TSID& tsid, int64_t t, double v)
    {
        if (app) return app->add(tsid, t, v);
        head->init_time(t);
        app = head->head_appender();
        return app->add(tsid, t, v);
    }

    error::Error commit()
    {
        if (!app) return error::Error();
        return app->commit();
    }

    error::Error rollback()
    {
        if (!app) return error::Error();
        return app->rollback();
    }

    ~InitAppender()
    {
        // LOG_DEBUG << "~InitAppender()";
    }
};

} // namespace head
} // namespace tsdb

#endif
