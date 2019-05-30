#ifndef INITAPPENDER_H
#define INITAPPENDER_H

#include "db/AppenderInterface.hpp"
#include "head/Head.hpp"
#include "head/HeadUtils.hpp"

namespace tsdb{
namespace head{

class InitAppender: public db::AppenderInterface{
    private:
        std::unique_ptr<db::AppenderInterface> app;
        Head * head;

    public:
        InitAppender(Head * head): head(head){}

        std::pair<uint64_t, error::Error> add(const label::Labels & lset, int64_t t, double v){
            if(app)
                return app->add(lset, t, v);
            head->init_time(t);
            app = head->head_appender();
            return app->add(lset, t, v);
        }

        error::Error add_fast(uint64_t ref, int64_t t, double v){
            if(!app)
                return ErrNotFound;
            return app->add_fast(ref, t, v);
        }

        error::Error commit(){
            if(!app)
                return error::Error();
            return app->commit();
        }

        error::Error rollback(){
            if(!app)
                return error::Error();
            return app->rollback();
        }

        ~InitAppender(){
            // LOG_DEBUG << "~InitAppender()";
        }
};

}}

#endif