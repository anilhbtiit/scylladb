/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#pragma once

#include <boost/intrusive/list.hpp>

#include <seastar/core/future.hh>
#include <stdexcept>
#include <exception>
#include <optional>
#include <seastar/core/timer.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_ptr.hh>

namespace seastar {

namespace internal {
// Test if a class T has member function broken()
template <typename T>
class has_broken {
    template <typename U> constexpr static bool check(decltype(&U::broken)) { return true; }
    template <typename U> constexpr static bool check(...) { return false; }

public:
    constexpr static bool value = check<T>(nullptr);
};
// Test if a class T has member function aborted()
template <typename T>
class has_aborted {
    template <typename U> constexpr static bool check(decltype(&U::aborted)) { return true; }
    template <typename U> constexpr static bool check(...) { return false; }

public:
    constexpr static bool value = check<T>(nullptr);
};
}

/// \addtogroup fiber-module
/// @{

/// Exception thrown when a semaphore is broken by
/// \ref semaphore::broken().
class broken_semaphore : public std::exception {
public:
    /// Reports the exception reason.
    virtual const char* what() const noexcept;
};

/// Exception thrown when a semaphore wait operation
/// times out.
///
/// \see semaphore::wait(typename timer<>::duration timeout, size_t nr)
class semaphore_timed_out : public timed_out_error {
public:
    /// Reports the exception reason.
    virtual const char* what() const noexcept;
};

/// Exception thrown when a semaphore wait operation
/// was aborted.
///
/// \see semaphore::wait(abort_source&, size_t nr)
class semaphore_aborted : public abort_requested_exception {
public:
    /// Reports the exception reason.
    virtual const char* what() const noexcept;
};

/// Exception Factory for standard semaphore
///
/// constructs standard semaphore exceptions
/// \see semaphore_timed_out and broken_semaphore
struct semaphore_default_exception_factory {
    static semaphore_timed_out timeout() noexcept;
    static broken_semaphore broken() noexcept;
    static semaphore_aborted aborted() noexcept;
};

class named_semaphore_timed_out : public semaphore_timed_out {
    sstring _msg;
public:
    named_semaphore_timed_out(std::string_view msg) noexcept;
    virtual const char* what() const noexcept;
};

class broken_named_semaphore : public broken_semaphore {
    sstring _msg;
public:
    broken_named_semaphore(std::string_view msg) noexcept;
    virtual const char* what() const noexcept;
};

class named_semaphore_aborted : public semaphore_aborted {
    sstring _msg;
public:
    named_semaphore_aborted(std::string_view msg) noexcept;
    virtual const char* what() const noexcept;
};

// A factory of semaphore exceptions that contain additional context: the semaphore name
// auto sem = named_semaphore(0, named_semaphore_exception_factory{"file_opening_limit_semaphore"});
struct named_semaphore_exception_factory {
    sstring name;
    named_semaphore_timed_out timeout() const noexcept;
    broken_named_semaphore broken() const noexcept;
    named_semaphore_aborted aborted() const noexcept;
};

template<typename ExceptionFactory, typename Clock>
class semaphore_units;

/// \brief Counted resource guard.
///
/// This is a standard computer science semaphore, adapted
/// for futures.  You can deposit units into a counter,
/// or take them away.  Taking units from the counter may wait
/// if not enough units are available.
///
/// To support exceptional conditions, a \ref broken() method
/// is provided, which causes all current waiters to stop waiting,
/// with an exceptional future returned.  This allows causing all
/// fibers that are blocked on a semaphore to continue.  This is
/// similar to POSIX's `pthread_cancel()`, with \ref wait() acting
/// as a cancellation point.
///
/// \tparam ExceptionFactory template parameter allows modifying a semaphore to throw
/// customized exceptions on timeout/broken/aborted. It has to provide three functions:
/// ExceptionFactory::timeout() - that returns a \ref semaphore_timed_out exception by default,
/// ExceptionFactory::broken() - that returns a \ref broken_semaphore exception by default, and
/// ExceptionFactory::aborted() - that returns a \ref semaphore_aborted exception by default.
template<typename ExceptionFactory, typename Clock = typename timer<>::clock>
class basic_semaphore : private ExceptionFactory {
public:
    using timer = typename seastar::timer<Clock>;
    using duration = typename timer::duration;
    using clock = typename timer::clock;
    using time_point = typename timer::time_point;
    using exception_factory = ExceptionFactory;
    using semaphore_units = seastar::semaphore_units<ExceptionFactory, Clock>;
private:
    struct state : public enable_lw_shared_from_this<state> {
        basic_semaphore* sem;
        ssize_t count;
        std::exception_ptr ex;

        state(basic_semaphore& sem_, ssize_t count_) noexcept : sem(&sem_), count(count_) {}
    };
    lw_shared_ptr<state> _state;

    class basic_waiter : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
    public:
        basic_semaphore* sem;
        size_t nr;

    private:
        std::variant<timer, optimized_optional<abort_source::subscription>> _aborter;

    public:
        basic_waiter() noexcept : sem(nullptr), nr(0) {}
        basic_waiter(basic_semaphore& sem_, size_t nr_) noexcept
            : sem(&sem_), nr(nr_)
        {
            sem->add_waiter(*this);
        }
        basic_waiter(basic_semaphore& sem_, size_t nr_, time_point timeout) noexcept
            : basic_waiter(sem_, nr_)
        {
            set_timeout(timeout);
        }
        basic_waiter(basic_semaphore& sem_, size_t nr_, abort_source& as) noexcept
            : basic_waiter(sem_, nr_)
        {
            subscribe(as);
        }
        virtual ~basic_waiter() = default;

        void init(basic_semaphore& sem_, size_t nr_) noexcept {
            sem = &sem_;
            nr = nr_;
            sem->add_waiter(*this);
        }

        void set_timeout(time_point timeout) noexcept {
            if (timeout == time_point::max()) {
                return;
            }
            auto& tmr = _aborter.template emplace<timer>([this] () {
                std::exception_ptr ex;
                try {
                    if (sem) {
                        ex = std::make_exception_ptr(sem->timeout());
                    }
                } catch (...) {
                }
                abort(ex ? std::move(ex) : std::make_exception_ptr(semaphore_timed_out()));
            });
            tmr.arm(timeout);
        }

        void subscribe(abort_source& as) noexcept {
            const auto& opt_sub = _aborter.template emplace<optimized_optional<abort_source::subscription>>(
                as.subscribe([this] (const std::optional<std::exception_ptr>& opt_ex) noexcept {
                    std::exception_ptr ex;
                    try {
                        if (opt_ex) {
                            ex = *opt_ex;
                        } else if (sem) {
                            if (sem->_state->ex) {
                                ex = sem->_state->ex;
                            } else if constexpr (internal::has_aborted<ExceptionFactory>::value) {
                                ex = std::make_exception_ptr(sem->aborted());
                            }
                        }
                    } catch (...) {
                    }
                    abort(ex ? std::move(ex) : std::make_exception_ptr(semaphore_aborted()));
                }
            ));
            assert(opt_sub);
        }

        void abort(std::exception_ptr ex) noexcept {
            set_exception(std::move(ex));
        }

        virtual void set_value() noexcept = 0;
        virtual void set_exception(std::exception_ptr) noexcept = 0;
    };

    boost::intrusive::list<basic_waiter, boost::intrusive::constant_time_size<false>> _wait_list;

    void add_waiter(basic_waiter& w) {
        _wait_list.push_back(w);
    }

    bool has_available_units(size_t nr) const noexcept {
        return _state->count >= 0 && (static_cast<size_t>(_state->count) >= nr);
    }
    bool may_proceed(size_t nr) const noexcept {
        return has_available_units(nr) && _wait_list.empty();
    }

    friend semaphore_units;
public:
    /// Returns the maximum number of units the semaphore counter can hold
    static constexpr size_t max_counter() noexcept {
        return std::numeric_limits<decltype(_state->count)>::max();
    }

    /// Constructs a semaphore object with a specific number of units
    /// in its internal counter. E.g., starting it at 1 is suitable for use as
    /// an unlocked mutex.
    ///
    /// \param count number of initial units present in the counter.
    basic_semaphore(size_t count) noexcept(std::is_nothrow_default_constructible_v<exception_factory>)
        : exception_factory()
        , _state(make_lw_shared<state>(*this, count))
    {}
    basic_semaphore(size_t count, exception_factory&& factory) noexcept(std::is_nothrow_move_constructible_v<exception_factory>)
        : exception_factory(std::move(factory))
        , _state(make_lw_shared<state>(*this, count))
    {}
    basic_semaphore(basic_semaphore&& o) noexcept
        : exception_factory(std::move(o))
        , _state(std::move(o._state))
        , _wait_list(std::move(o._wait_list))
        , _waiters_freelist(std::move(o._waiters_freelist))
    {
        if (_state->sem) {
            _state->sem = this;
        }
    }
    basic_semaphore& operator=(basic_semaphore&& o) noexcept {
        if (this != &o) {
            broken();
            exception_factory::operator=(std::move(o));
            _state = std::move(o._state);
            if (_state->sem) {
                _state->sem = this;
            }
            _wait_list = std::move(o._wait_list);
            _waiters_freelist.splice(_waiters_freelist.end(), o._waiters_freelist);
        }
        return *this;
    }

    ~basic_semaphore() {
        broken();
    }

private:
    struct waiter : public basic_waiter {
        using basic_waiter::basic_waiter;

        promise<> pr;

        future<> get_future() noexcept { return pr.get_future(); }
        virtual void set_value() noexcept override {
            pr.set_value();
            // note: we self-delete in either case we are woken
            // up. See usage below: only the resulting future
            // state is required once we've left the _wait_list
            delete this;
        }
        virtual void set_exception(std::exception_ptr ex) noexcept override {
            pr.set_exception(std::move(ex));
            // note: we self-delete in either case we are woken
            // up. See usage below: only the resulting future
            // state is required once we've left the _wait_list
            delete this;
        }
    };

    boost::intrusive::list<waiter, boost::intrusive::constant_time_size<false>> _waiters_freelist;

    waiter* new_waiter(basic_semaphore& sem, size_t nr) {
        if (_waiters_freelist.empty()) {
            return new waiter(sem, nr);
        }
        auto& w = _waiters_freelist.front();
        _waiters_freelist.pop_front();
        w.init(sem, nr);
        return &w;
    }

public:
    /// Waits until at least a specific number of units are available in the
    /// counter, and reduces the counter by that amount of units.
    ///
    /// \note Waits are serviced in FIFO order, though if several are awakened
    ///       at once, they may be reordered by the scheduler.
    ///
    /// \param nr Amount of units to wait for (default 1).
    /// \return a future that becomes ready when sufficient units are available
    ///         to satisfy the request.  If the semaphore was \ref broken(), may
    ///         contain an exception.
    future<> wait(size_t nr = 1) noexcept {
        if (may_proceed(nr)) {
            _state->count -= nr;
            return make_ready_future<>();
        }
        if (_state->ex) {
            return make_exception_future(_state->ex);
        }
        try {
            auto* w = new_waiter(*this, nr);
            return w->get_future();
        } catch (...) {
            return current_exception_as_future();
        }
    }
    /// Waits until at least a specific number of units are available in the
    /// counter, and reduces the counter by that amount of units.  If the request
    /// cannot be satisfied in time, the request is aborted.
    ///
    /// \note Waits are serviced in FIFO order, though if several are awakened
    ///       at once, they may be reordered by the scheduler.
    ///
    /// \param timeout expiration time.
    /// \param nr Amount of units to wait for (default 1).
    /// \return a future that becomes ready when sufficient units are available
    ///         to satisfy the request.  On timeout, the future contains a
    ///         \ref semaphore_timed_out exception.  If the semaphore was
    ///         \ref broken(), may contain a \ref broken_semaphore exception.
    future<> wait(time_point timeout, size_t nr = 1) noexcept {
        if (may_proceed(nr)) {
            _state->count -= nr;
            return make_ready_future<>();
        }
        if (_state->ex) {
            return make_exception_future(_state->ex);
        }
        if (Clock::now() >= timeout) {
            try {
                return make_exception_future(this->timeout());
            } catch (...) {
                return make_exception_future(semaphore_timed_out());
            }
        }
        try {
            auto* w = new_waiter(*this, nr);
            w->set_timeout(timeout);
            return w->get_future();
        } catch (...) {
            return current_exception_as_future();
        }
    }

    /// Waits until at least a specific number of units are available in the
    /// counter, and reduces the counter by that amount of units.  The request
    /// can be aborted using an \ref abort_source.
    ///
    /// \note Waits are serviced in FIFO order, though if several are awakened
    ///       at once, they may be reordered by the scheduler.
    ///
    /// \param as abort source.
    /// \param nr Amount of units to wait for (default 1).
    /// \return a future that becomes ready when sufficient units are available
    ///         to satisfy the request.  On abort, the future contains a
    ///         \ref semaphore_aborted exception.  If the semaphore was
    ///         \ref broken(), may contain a \ref broken_semaphore exception.
    future<> wait(abort_source& as, size_t nr = 1) noexcept {
        if (may_proceed(nr)) {
            _state->count -= nr;
            return make_ready_future<>();
        }
        if (_state->ex) {
            return make_exception_future(_state->ex);
        }
        if (as.abort_requested()) {
            if constexpr (internal::has_aborted<ExceptionFactory>::value) {
                try {
                    return make_exception_future(this->aborted());
                } catch (...) {
                    return make_exception_future(semaphore_aborted());
                }
            } else {
                return make_exception_future(semaphore_aborted());
            }
        }
        try {
            auto* w = new_waiter(*this, nr);
            w->subscribe(as);
            return w->get_future();
        } catch (...) {
            return current_exception_as_future();
        }
    }

    /// Waits until at least a specific number of units are available in the
    /// counter, and reduces the counter by that amount of units.  If the request
    /// cannot be satisfied in time, the request is aborted.
    ///
    /// \note Waits are serviced in FIFO order, though if several are awakened
    ///       at once, they may be reordered by the scheduler.
    ///
    /// \param timeout how long to wait.
    /// \param nr Amount of units to wait for (default 1).
    /// \return a future that becomes ready when sufficient units are available
    ///         to satisfy the request.  On timeout, the future contains a
    ///         \ref semaphore_timed_out exception.  If the semaphore was
    ///         \ref broken(), may contain a \ref broken_semaphore exception.
    future<> wait(duration timeout, size_t nr = 1) noexcept {
        return wait(clock::now() + timeout, nr);
    }

private:
    struct units_waiter : public basic_waiter {
        using basic_waiter::basic_waiter;

        promise<semaphore_units> pr;

        future<semaphore_units> get_future() noexcept { return pr.get_future(); }
        virtual void set_value() noexcept override {
            pr.set_value(semaphore_units(*this->sem, this->nr));
            // note: we self-delete in either case we are woken
            // up. See usage below: only the resulting future
            // state is required once we've left the _wait_list
            delete this;
        }
        virtual void set_exception(std::exception_ptr ex) noexcept override {
            pr.set_exception(std::move(ex));
            // note: we self-delete in either case we are woken
            // up. See usage below: only the resulting future
            // state is required once we've left the _wait_list
            delete this;
        }
    };

public:
    /// Waits until at least a specific number of units are available in the
    /// counter, and reduces the counter by that amount of units.
    ///
    /// \note Waits are serviced in FIFO order, though if several are awakened
    ///       at once, they may be reordered by the scheduler.
    ///
    /// \param nr Amount of units to wait for (default 1).
    /// \return a future holding \ref semaphore_units that becomes ready when sufficient units are available
    ///         to satisfy the request.  If the semaphore was \ref broken(), may
    ///         contain an exception.
    future<semaphore_units> get_units(size_t nr = 1) noexcept {
        if (may_proceed(nr)) {
            _state->count -= nr;
            return make_ready_future<semaphore_units>(*this, nr);
        }
        if (_state->ex) {
            return make_exception_future<semaphore_units>(_state->ex);
        }
        try {
            auto* w = new units_waiter(*this, nr);
            return w->get_future();
        } catch (...) {
            return current_exception_as_future<semaphore_units>();
        }
    }
    /// Waits until at least a specific number of units are available in the
    /// counter, and reduces the counter by that amount of units.  If the request
    /// cannot be satisfied in time, the request is aborted.
    ///
    /// \note Waits are serviced in FIFO order, though if several are awakened
    ///       at once, they may be reordered by the scheduler.
    ///
    /// \param timeout expiration time.
    /// \param nr Amount of units to wait for (default 1).
    /// \return a future holding \ref semaphore_units that becomes ready when sufficient units are available
    ///         to satisfy the request.  On timeout, the future contains a
    ///         \ref semaphore_timed_out exception.  If the semaphore was
    ///         \ref broken(), may contain a \ref broken_semaphore exception.
    future<semaphore_units> get_units(time_point timeout, size_t nr = 1) noexcept {
        if (may_proceed(nr)) {
            _state->count -= nr;
            return make_ready_future<semaphore_units>(*this, nr);
        }
        if (_state->ex) {
            return make_exception_future<semaphore_units>(_state->ex);
        }
        if (Clock::now() >= timeout) {
            try {
                return make_exception_future<semaphore_units>(this->timeout());
            } catch (...) {
                return make_exception_future<semaphore_units>(semaphore_timed_out());
            }
        }
        try {
            auto* w = new units_waiter(*this, nr, timeout);
            return w->get_future();
        } catch (...) {
            return current_exception_as_future<semaphore_units>();
        }
    }

    /// Waits until at least a specific number of units are available in the
    /// counter, and reduces the counter by that amount of units.  If the request
    /// cannot be satisfied in time, the request is aborted.
    ///
    /// \note Waits are serviced in FIFO order, though if several are awakened
    ///       at once, they may be reordered by the scheduler.
    ///
    /// \param timeout how long to wait.
    /// \param nr Amount of units to wait for (default 1).
    /// \return a future holding \ref semaphore_units that becomes ready when sufficient units are available
    ///         to satisfy the request.  On timeout, the future contains a
    ///         \ref semaphore_timed_out exception.  If the semaphore was
    ///         \ref broken(), may contain a \ref broken_semaphore exception.
    future<semaphore_units> get_units(duration timeout, size_t nr = 1) noexcept {
        return get_units(clock::now() + timeout, nr);
    }

    /// Waits until at least a specific number of units are available in the
    /// counter, and reduces the counter by that amount of units.  The request
    /// can be aborted using an \ref abort_source.
    ///
    /// \note Waits are serviced in FIFO order, though if several are awakened
    ///       at once, they may be reordered by the scheduler.
    ///
    /// \param as abort source.
    /// \param nr Amount of units to wait for (default 1).
    /// \return a future holding \ref semaphore_units that becomes ready when sufficient units are available
    ///         to satisfy the request.  On abort, the future contains a
    ///         \ref semaphore_aborted exception.  If the semaphore was
    ///         \ref broken(), may contain a \ref broken_semaphore exception.
    future<semaphore_units> get_units(abort_source& as, size_t nr = 1) noexcept {
        if (may_proceed(nr)) {
            _state->count -= nr;
            return make_ready_future<semaphore_units>(*this, nr);
        }
        if (_state->ex) {
            return make_exception_future<semaphore_units>(_state->ex);
        }
        if (as.abort_requested()) {
            if constexpr (internal::has_aborted<ExceptionFactory>::value) {
                try {
                    return make_exception_future<semaphore_units>(this->aborted());
                } catch (...) {
                    return make_exception_future<semaphore_units>(semaphore_aborted());
                }
            } else {
                return make_exception_future<semaphore_units>(semaphore_aborted());
            }
        }
        try {
            auto* w = new units_waiter(*this, nr, as);
            return w->get_future();
        } catch (...) {
            return current_exception_as_future<semaphore_units>();
        }
    }

    /// Deposits a specified number of units into the counter.
    ///
    /// The counter is incremented by the specified number of units.
    /// If the new counter value is sufficient to satisfy the request
    /// of one or more waiters, their futures (in FIFO order) become
    /// ready, and the value of the counter is reduced according to
    /// the amount requested.
    ///
    /// \param nr Number of units to deposit (default 1).
    void signal(size_t nr = 1) noexcept {
        if (_state->ex) {
            return;
        }
        _state->count += nr;
        while (!_wait_list.empty() && has_available_units(_wait_list.front().nr)) {
            auto& x = _wait_list.front();
            _state->count -= x.nr;
            // set_value sets the promise value
            // and deletes the object
            x.set_value();
        }
    }

    /// Consume the specific number of units without blocking
    //
    /// Consume the specific number of units now, regardless of how many units are available
    /// in the counter, and reduces the counter by that amount of units. This operation may
    /// cause the counter to go negative.
    ///
    /// \param nr Amount of units to consume (default 1).
    void consume(size_t nr = 1) noexcept {
        if (_state->ex) {
            return;
        }
        _state->count -= nr;
    }

    /// Attempts to reduce the counter value by a specified number of units.
    ///
    /// If sufficient units are available in the counter, and if no
    /// other fiber is waiting, then the counter is reduced.  Otherwise,
    /// nothing happens.  This is useful for "opportunistic" waits where
    /// useful work can happen if the counter happens to be ready, but
    /// when it is not worthwhile to wait.
    ///
    /// \param nr number of units to reduce the counter by (default 1).
    /// \return `true` if the counter had sufficient units, and was decremented.
    bool try_wait(size_t nr = 1) noexcept {
        if (may_proceed(nr)) {
            _state->count -= nr;
            return true;
        } else {
            return false;
        }
    }
    /// Returns the number of units available in the counter.
    ///
    /// Does not take into account any waiters.
    size_t current() const noexcept { return std::max(_state->count, ssize_t(0)); }

    /// Returns the number of available units.
    ///
    /// Takes into account units consumed using \ref consume() and therefore
    /// may return a negative value.
    ssize_t available_units() const noexcept { return _state->count; }

    /// Returns the current number of waiters
    size_t waiters() const noexcept { return _wait_list.size(); }

    /// Signal to waiters that an error occurred.  \ref wait() will see
    /// an exceptional future<> containing a \ref broken_semaphore exception.
    /// The future is made available immediately.
    void broken() noexcept {
        std::exception_ptr ep;
        if constexpr (internal::has_broken<exception_factory>::value) {
            try {
                ep = std::make_exception_ptr(exception_factory::broken());
            } catch (...) {
                ep = std::make_exception_ptr(broken_semaphore());
            }
        } else {
            ep = std::make_exception_ptr(broken_semaphore());
        }
        broken(std::move(ep));
    }

    /// Signal to waiters that an error occurred.  \ref wait() will see
    /// an exceptional future<> containing the provided exception parameter.
    /// The future is made available immediately.
    template <typename Exception>
    void broken(const Exception& ex) noexcept {
        broken(std::make_exception_ptr(ex));
    }

    /// Signal to waiters that an error occurred.  \ref wait() will see
    /// an exceptional future<> containing the provided exception parameter.
    /// The future is made available immediately.
    void broken(std::exception_ptr ex) noexcept;

    /// Reserve memory for waiters so that wait() will not throw.
    void ensure_space_for_waiters(size_t n) {
        while (n--) {
            auto* w = new waiter();
            _waiters_freelist.push_back(*w);
        }
    }
};

template<typename ExceptionFactory, typename Clock>
inline
void
basic_semaphore<ExceptionFactory, Clock>::broken(std::exception_ptr xp) noexcept {
    static_assert(std::is_nothrow_copy_constructible_v<std::exception_ptr>);
    if (_state) {
        _state->ex = xp;
        _state->count = 0;
        _state->sem = nullptr;
    }
    while (!_wait_list.empty()) {
        auto& x = _wait_list.front();
        // set_exception passes the exception to the promise->future
        // and deletes the object
        x.set_exception(xp);
    }
    while (!_waiters_freelist.empty()) {
        auto* p = &_waiters_freelist.front();
        _waiters_freelist.pop_front();
        delete p;
    }
}

template<typename ExceptionFactory = semaphore_default_exception_factory, typename Clock = typename timer<>::clock>
class semaphore_units {
public:
    using basic_semaphore = seastar::basic_semaphore<ExceptionFactory, Clock>;

private:
    using state = typename basic_semaphore::state;

    lw_shared_ptr<state> _state;
    size_t _n = 0;

    semaphore_units(lw_shared_ptr<state> state, size_t n) noexcept : _state(std::move(state)), _n(n) {}
public:
    semaphore_units() = default;
    semaphore_units(basic_semaphore& sem, size_t n) noexcept : semaphore_units(sem._state, n) {}
    semaphore_units(semaphore_units&& o) noexcept : _state(std::move(o._state)), _n(std::exchange(o._n, 0)) {
    }
    semaphore_units& operator=(semaphore_units&& o) noexcept {
        if (!broken()) {
            return_all();
        }
        _state = std::move(o._state);
        _n = std::exchange(o._n, 0);
        return *this;
    }
    semaphore_units(const semaphore_units&) = delete;
    ~semaphore_units() noexcept {
      if (!broken()) {
        return_all();
      }
    }
    /// Return ownership of some units to the semaphore. The semaphore will be signaled by the number of units returned.
    ///
    /// \param units number of units to subtract.
    ///
    /// \note throws exception if \c units is more than those protected by the semaphore
    ///
    /// \return the number of remaining units
    size_t return_units(size_t units) {
        if (units > _n) {
            throw std::invalid_argument("Cannot take more units than those protected by the semaphore");
        }
        check();
        _n -= units;
        _state->sem->signal(units);
        return _n;
    }
    /// Return ownership of all units. The semaphore will be signaled by the number of units returned.
    void return_all() {
        check();
        if (_n) {
            _state->sem->signal(_n);
            _n = 0;
        }
    }
    /// Releases ownership of the units. The semaphore will not be signalled.
    ///
    /// \return the number of units held
    size_t release() noexcept {
        return std::exchange(_n, 0);
    }
    /// Splits this instance into a \ref semaphore_units object holding the specified amount of units.
    /// This object will continue holding the remaining units.
    ///
    /// \param units number of units to subtract.
    ///
    /// \note throws exception if \c units is more than those protected by the semaphore
    ///
    /// \return semaphore_units holding the specified number of units
    semaphore_units split(size_t units) {
        if (units > _n) {
            throw std::invalid_argument("Cannot take more units than those protected by the semaphore");
        }
        check();
        _n -= units;
        return semaphore_units(_state, units);
    }
    /// The inverse of split(), in which the units held by the specified \ref semaphore_units
    /// object are merged into the current one. The function assumes (and asserts) that both
    /// are associated with the same \ref semaphore.
    ///
    /// \return the updated semaphore_units object
    void adopt(semaphore_units&& other) noexcept {
        assert(other._state == _state);
        _n += other.release();
    }

    /// Returns the number of units held
    size_t count() const noexcept {
        return _n;
    }

    /// Returns true iff there any units held
    explicit operator bool() const noexcept {
        return _n != 0;
    }

    bool broken() const noexcept {
        return !_state || !_state->sem;
    }

    void check() const {
        if (broken()) {
            std::rethrow_exception(get_exception());
        }
    }

    std::exception_ptr get_exception() const noexcept {
        return _state ? _state->ex : std::make_exception_ptr(std::runtime_error("Uninitialized semaphore_units"));
    }
};

/// \brief Take units from semaphore temporarily
///
/// Takes units from the semaphore and returns them when the \ref semaphore_units object goes out of scope.
/// This provides a safe way to temporarily take units from a semaphore and ensure
/// that they are eventually returned under all circumstances (exceptions, premature scope exits, etc).
///
/// Unlike with_semaphore(), the scope of unit holding is not limited to the scope of a single async lambda.
///
/// \param sem The semaphore to take units from
/// \param units  Number of units to take
/// \return a \ref future<> holding \ref semaphore_units object. When the object goes out of scope
///         the units are returned to the semaphore.
///
/// \note The caller must guarantee that \c sem is valid as long as
///      \ref seaphore_units object is alive.
///
/// \related semaphore
template<typename ExceptionFactory, typename Clock = typename timer<>::clock>
future<semaphore_units<ExceptionFactory, Clock>>
get_units(basic_semaphore<ExceptionFactory, Clock>& sem, size_t units) noexcept {
    return sem.get_units(units);
}

/// \brief Take units from semaphore temporarily with time bound on wait
///
/// Like \ref get_units(basic_semaphore<ExceptionFactory>&, size_t) but when
/// timeout is reached before units are granted, returns an exceptional future holding \ref semaphore_timed_out.
///
/// \param sem The semaphore to take units from
/// \param units  Number of units to take
/// \return a \ref future<> holding \ref semaphore_units object. When the object goes out of scope
///         the units are returned to the semaphore.
///         If a timeout is reached before units are granted, returns an exceptional future holding \ref semaphore_timed_out.
///
/// \note The caller must guarantee that \c sem is valid as long as
///      \ref seaphore_units object is alive.
///
/// \related semaphore
template<typename ExceptionFactory, typename Clock = typename timer<>::clock>
future<semaphore_units<ExceptionFactory, Clock>>
get_units(basic_semaphore<ExceptionFactory, Clock>& sem, size_t units, typename basic_semaphore<ExceptionFactory, Clock>::time_point timeout) noexcept {
    return sem.get_units(timeout, units);
}

/// \brief Take units from semaphore temporarily with time bound on wait
///
/// Like \ref get_units(basic_semaphore<ExceptionFactory>&, size_t, basic_semaphore<ExceptionFactory>::time_point) but
/// allow the timeout to be specified as a duration.
///
/// \param sem The semaphore to take units from
/// \param units  Number of units to take
/// \param timeout a duration specifying when to timeout the current request
/// \return a \ref future<> holding \ref semaphore_units object. When the object goes out of scope
///         the units are returned to the semaphore.
///         If a timeout is reached before units are granted, returns an exceptional future holding \ref semaphore_timed_out.
///
/// \note The caller must guarantee that \c sem is valid as long as
///      \ref seaphore_units object is alive.
///
/// \related semaphore
template<typename ExceptionFactory, typename Clock>
future<semaphore_units<ExceptionFactory, Clock>>
get_units(basic_semaphore<ExceptionFactory, Clock>& sem, size_t units, typename basic_semaphore<ExceptionFactory, Clock>::duration timeout) noexcept {
    return sem.get_units(timeout, units);
}

/// \brief Take units from semaphore temporarily with an \ref abort_source
///
/// Like \ref get_units(basic_semaphore<ExceptionFactory>&, size_t, basic_semaphore<ExceptionFactory>::time_point) but
/// allow the function to be aborted using an \ref abort_source.
///
/// \param sem The semaphore to take units from
/// \param units  Number of units to take
/// \param as abort source.
/// \return a \ref future<> holding \ref semaphore_units object. When the object goes out of scope
///         the units are returned to the semaphore.
///         If get_units is aborted before units are granted, returns an exceptional future holding \ref semaphore_aborted.
///
/// \note The caller must guarantee that \c sem is valid as long as
///      \ref seaphore_units object is alive.
///
/// \related semaphore
template<typename ExceptionFactory, typename Clock>
future<semaphore_units<ExceptionFactory, Clock>>
get_units(basic_semaphore<ExceptionFactory, Clock>& sem, size_t units, abort_source& as) noexcept {
    return sem.get_units(as, units);
}

/// \brief Try to take units from semaphore temporarily
///
/// Takes units from the semaphore, if available, and returns them when the \ref semaphore_units object goes out of scope.
/// This provides a safe way to temporarily take units from a semaphore and ensure
/// that they are eventually returned under all circumstances (exceptions, premature scope exits, etc).
///
/// Unlike with_semaphore(), the scope of unit holding is not limited to the scope of a single async lambda.
///
/// \param sem The semaphore to take units from
/// \param units  Number of units to take
/// \return an optional \ref semaphore_units object. If engaged, when the object goes out of scope or is reset
///         the units are returned to the semaphore.
///
/// \note The caller must guarantee that \c sem is valid as long as
///      \ref seaphore_units object is alive.
///
/// \related semaphore
template<typename ExceptionFactory, typename Clock = typename timer<>::clock>
std::optional<semaphore_units<ExceptionFactory, Clock>>
try_get_units(basic_semaphore<ExceptionFactory, Clock>& sem, size_t units) noexcept {
    if (!sem.try_wait(units)) {
        return std::nullopt;
    }
    return std::make_optional<semaphore_units<ExceptionFactory, Clock>>(sem, units);
}

/// \brief Consume units from semaphore temporarily
///
/// Consume units from the semaphore and returns them when the \ref semaphore_units object goes out of scope.
/// This provides a safe way to temporarily take units from a semaphore and ensure
/// that they are eventually returned under all circumstances (exceptions, premature scope exits, etc).
///
/// Unlike get_units(), this calls the non-blocking consume() API.
///
/// Unlike with_semaphore(), the scope of unit holding is not limited to the scope of a single async lambda.
///
/// \param sem The semaphore to take units from
/// \param units  Number of units to consume
template<typename ExceptionFactory, typename Clock = typename timer<>::clock>
semaphore_units<ExceptionFactory, Clock>
consume_units(basic_semaphore<ExceptionFactory, Clock>& sem, size_t units) noexcept {
    sem.consume(units);
    return semaphore_units<ExceptionFactory, Clock>{ sem, units };
}

/// \brief Runs a function protected by a semaphore
///
/// Acquires a \ref semaphore, runs a function, and releases
/// the semaphore, returning the the return value of the function,
/// as a \ref future.
///
/// \param sem The semaphore to be held while the \c func is
///            running.
/// \param units  Number of units to acquire from \c sem (as
///               with semaphore::wait())
/// \param func   The function to run; signature \c void() or
///               \c future<>().
/// \return a \ref future<> holding the function's return value
///         or exception thrown; or a \ref future<> containing
///         an exception from one of the semaphore::broken()
///         variants.
///
/// \note The caller must guarantee that \c sem is valid until
///       the future returned by with_semaphore() resolves.
///
/// \related semaphore
template <typename ExceptionFactory, typename Func, typename Clock = typename timer<>::clock>
inline
futurize_t<std::invoke_result_t<Func>>
with_semaphore(basic_semaphore<ExceptionFactory, Clock>& sem, size_t units, Func&& func) noexcept {
    return sem.wait(units).then([&sem, units, func = std::forward<Func>(func)] () mutable {
        return futurize_invoke(std::forward<Func>(func)).finally([&sem, units] {
            sem.signal(units);
        });
    });
}

/// \brief Runs a function protected by a semaphore with time bound on wait
///
/// If possible, acquires a \ref semaphore, runs a function, and releases
/// the semaphore, returning the the return value of the function,
/// as a \ref future.
///
/// If the semaphore can't be acquired within the specified timeout, returns
/// a semaphore_timed_out exception
///
/// \param sem The semaphore to be held while the \c func is
///            running.
/// \param units  Number of units to acquire from \c sem (as
///               with semaphore::wait())
/// \param timeout a duration specifying when to timeout the current request
/// \param func   The function to run; signature \c void() or
///               \c future<>().
/// \return a \ref future<> holding the function's return value
///         or exception thrown; or a \ref future<> containing
///         an exception from one of the semaphore::broken()
///         variants.
///
/// \note The caller must guarantee that \c sem is valid until
///       the future returned by with_semaphore() resolves.
///
/// \related semaphore
template <typename ExceptionFactory, typename Clock, typename Func>
inline
futurize_t<std::invoke_result_t<Func>>
with_semaphore(basic_semaphore<ExceptionFactory, Clock>& sem, size_t units, typename basic_semaphore<ExceptionFactory, Clock>::duration timeout, Func&& func) noexcept {
    return sem.wait(units, timeout).then([&sem, units, func = std::forward<Func>(func)] () mutable {
        return futurize_invoke(std::forward<Func>(func)).finally([&sem, units] {
            sem.signal(units);
        });
    });
}

/// default basic_semaphore specialization that throws semaphore specific exceptions
/// on error conditions.
using semaphore = basic_semaphore<semaphore_default_exception_factory>;
using named_semaphore = basic_semaphore<named_semaphore_exception_factory>;

/// @}

}
