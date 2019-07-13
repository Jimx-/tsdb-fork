#ifndef APPENDERINTERFACE_H
#define APPENDERINTERFACE_H

#include "base/Error.hpp"
#include "tagtree/tsid.h"
#include "label/Label.hpp"

#include <vector>

namespace tsdb {
namespace db {

// Appender allows appending a batch of data. It must be completed with a
// call to commit or rollback and must not be reused afterwards.
//
// Operations on the Appender interface are not thread-safe.
class AppenderInterface {
public:
    // add adds a sample pair for the given series. A reference number is
    // returned which can be used to add further samples in the same or later
    // transactions.
    // Returned reference numbers are ephemeral and may be rejected in calls
    // to AddFast() at any point. Adding the sample via add() returns a new
    // reference number.
    // If the reference is 0 it must not be used for caching.
    virtual error::Error add(const tagtree::TSID& tsid, int64_t t, double v) = 0;
    // Return a gorup id.
    virtual error::Error add_group(const std::vector<tagtree::TSID>& tsids,
                                   int64_t t, const std::vector<double>& v)
    {
        return error::Error();
    }

    // commit submits the collected samples and purges the batch.
    virtual error::Error commit() = 0;

    // rollback rolls back all modifications made in the appender so far.
    virtual error::Error rollback() = 0;

    virtual ~AppenderInterface() = default;
};

} // namespace db
} // namespace tsdb

#endif
