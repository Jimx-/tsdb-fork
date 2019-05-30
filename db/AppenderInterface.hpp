#ifndef APPENDERINTERFACE_H
#define APPENDERINTERFACE_H

#include "base/Error.hpp"
#include "label/Label.hpp"

namespace tsdb{
namespace db{

// Appender allows appending a batch of data. It must be completed with a
// call to commit or rollback and must not be reused afterwards.
//
// Operations on the Appender interface are not thread-safe.
class AppenderInterface{
    public:
        // add adds a sample pair for the given series. A reference number is
        // returned which can be used to add further samples in the same or later
        // transactions.
        // Returned reference numbers are ephemeral and may be rejected in calls
        // to AddFast() at any point. Adding the sample via add() returns a new
        // reference number.
        // If the reference is 0 it must not be used for caching.
        virtual std::pair<uint64_t, error::Error> add(const label::Labels & lset, int64_t t, double v)=0;
        // Return a gorup id.
        virtual std::pair<uint64_t, error::Error> add_group(const std::deque<label::Labels> & labels, int64_t t, const std::deque<double> & v){ return {0, error::Error()}; }

        // add_fast adds a sample pair for the referenced series. It is generally faster
        // than adding a sample by providing its full label set.
        virtual error::Error add_fast(uint64_t ref, int64_t t, double v)=0;
        virtual error::Error add_group_fast(uint64_t group_id, int64_t t, const std::deque<double> & v){ return error::Error(); }

        // commit submits the collected samples and purges the batch.
        virtual error::Error commit()=0;

        // rollback rolls back all modifications made in the appender so far.
        virtual error::Error rollback()=0;

        virtual ~AppenderInterface()=default;
};

}}

#endif