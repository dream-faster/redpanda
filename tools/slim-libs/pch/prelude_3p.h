// POC: precompiled prelude of the third-party headers most translation units
// in src/v include. Used only to bound how much a "library is already parsed"
// strategy could save; not wired into the Bazel build.
#pragma once

// std
#include <algorithm>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

// fmt
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <fmt/std.h>

// seastar
#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/variant_utils.hh>

// boost
#include <boost/intrusive/list.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/outcome/basic_result.hpp>
#include <boost/outcome/std_outcome.hpp>
#include <boost/outcome/std_result.hpp>
#include <boost/range/numeric.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/iterator/counting_iterator.hpp>

// absl
#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/inlined_vector.h>
#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>
#include <absl/hash/hash.h>
#include <absl/time/time.h>
#include <absl/types/span.h>

// others
#include <ankerl/unordered_dense.h>
#include <yaml-cpp/yaml.h>
