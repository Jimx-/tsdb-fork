#ifndef _TSID_H_
#define _TSID_H_

#include "base/Logging.hpp"

#include <functional>
#include <string>
#include <uuid/uuid.h>

namespace tsdb {
namespace common {

/* time series identifier type */
class TSID {
public:
    TSID() { uuid_generate(uuid); }
    TSID(const char* uuid_in) { uuid_parse(uuid_in, uuid); }
    TSID(std::string_view uuid_in) { uuid_parse(uuid_in.data(), uuid); }
    TSID(const TSID& rhs) { uuid_copy(uuid, rhs.uuid); }

    bool operator==(const TSID& rhs) const
    {
        return !uuid_compare(uuid, rhs.uuid);
    }

    bool operator!=(const TSID& rhs) const { return !(*this == rhs); }

    std::string to_string() const
    {
        char buf[36];
        uuid_unparse(uuid, buf);

        return std::string(buf);
    }

private:
    uuid_t uuid;
};

} // namespace common
} // namespace tsdb

namespace std {

template <> class hash<tsdb::common::TSID> {
public:
    size_t operator()(const tsdb::common::TSID& tsid) const
    {
        return std::hash<std::string>()(tsid.to_string());
    }
};

} // namespace std

#endif
