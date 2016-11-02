/*
 * Copyright 2015 Cloudius Systems
 */

#ifndef CORE_SCOLLECTD_API_HH_
#define CORE_SCOLLECTD_API_HH_

#include "core/scollectd.hh"

namespace scollectd {

struct collectd_value {
    union {
        double _d;
        uint64_t _ui;
        int64_t _i;
    } u;
    scollectd::data_type _type;
    collectd_value()
            : _type(data_type::GAUGE) {
    }
    collectd_value(data_type t, uint64_t i)
            : _type(t) {
        u._ui = i;
    }

    scollectd::data_type type() const {
        return _type;
    }

    double d() const {
        return u._d;
    }

    uint64_t ui() const {
        return u._ui;
    }
    int64_t i() const {
        return u._i;
    }

    collectd_value& operator=(const collectd_value& c) = default;

    collectd_value& operator+=(const collectd_value& c) {
        *this = *this + c;
        return *this;
    }

    collectd_value operator+(const collectd_value& c) {
        collectd_value res(*this);
        switch (_type) {
        case data_type::GAUGE:
            res.u._d += c.u._d;
            break;
        case data_type::DERIVE:
            res.u._i += c.u._i;
            break;
        default:
            res.u._ui += c.u._ui;
            break;
        }
        return res;
    }
};
typedef std::map<type_instance_id, std::vector<collectd_value> > value_map;

std::vector<collectd_value> get_collectd_value(
        const scollectd::type_instance_id& id);

std::vector<scollectd::type_instance_id> get_collectd_ids();

sstring get_collectd_description_str(const scollectd::type_instance_id&);

bool is_enabled(const scollectd::type_instance_id& id);
/**
 * Enable or disable collectd metrics on local instance
 * @id - the metric to enable or disable
 * @enable - should the collectd metrics be enable or disable
 */
void enable(const scollectd::type_instance_id& id, bool enable);


value_map get_value_map();
}

#endif /* CORE_SCOLLECTD_API_HH_ */
