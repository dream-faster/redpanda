// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition.h"

#include "base/format_to.h"
#include "base/outcome.h"
#include "cluster/id_allocator_stm.h"
#include "cluster/log_eviction_stm.h"
#include "cluster/logger.h"
#include "cluster/partition_properties_stm.h"
#include "cluster/rm_stm.h"
#include "cluster/tm_stm.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "raft/fundamental.h"
#include "raft/fwd.h"
#include "raft/state_machine_manager.h"
#include "ssx/future-util.h"
#include "ssx/when_all.h"
#include "storage/ntp_config.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <optional>

namespace cluster {

partition::partition(
  consensus_ptr r, ss::sharded<features::feature_table>& feature_table)
  : _raft(std::move(r))
  , _probe(std::make_unique<replicated_partition_probe>(*this))
  , _feature_table(feature_table)
  , _log_cleanup_policy(config::shard_local_cfg().log_cleanup_policy.bind()) {
    _log_cleanup_policy.watch([this]() {
        if (_as.abort_requested()) {
            return ss::now();
        }
        _raft->log()->notify_compaction_update();
        return ss::now();
    });
}

ss::future<std::error_code> partition::prefix_truncate(
  model::offset rp_start_offset,
  kafka::offset kafka_start_offset,
  ss::lowres_clock::time_point deadline) {
    if (!_log_eviction_stm || !_raft->log_config().is_locally_collectable()) {
        vlog(
          clusterlog.info,
          "Cannot prefix-truncate topic/partition {} retention settings not "
          "applied",
          _raft->ntp());
        co_return make_error_code(errc::topic_invalid_config);
    }
    if (!feature_table().local().is_active(features::feature::delete_records)) {
        vlog(
          clusterlog.info,
          "Cannot prefix-truncate topic/partition {} feature is currently "
          "disabled",
          _raft->ntp());
        co_return make_error_code(cluster::errc::feature_disabled);
    }
    vlog(
      clusterlog.info,
      "Truncating {} to redpanda offset {} kafka offset {}",
      _raft->ntp(),
      rp_start_offset,
      kafka_start_offset);
    auto res = co_await _log_eviction_stm->truncate(
      rp_start_offset, kafka_start_offset, deadline, _as);
    if (res.has_error()) {
        co_return res.error();
    }
    co_return errc::success;
}

ss::future<result<kafka_result>> partition::replicate(
  chunked_vector<model::record_batch> batches, raft::replicate_options opts) {
    using ret_t = result<kafka_result>;

    auto maybe_units = co_await hold_writes_enabled();
    if (!maybe_units) {
        co_return ret_t(maybe_units.error());
    }

    auto res = co_await _raft->replicate(std::move(batches), opts);
    if (!res) {
        co_return ret_t(res.error());
    }
    co_return ret_t(
      kafka_result{
        model::offset_cast(log()->from_log_offset(res.value().last_offset)),
        res.value().last_term});
}

ss::shared_ptr<cluster::rm_stm> partition::rm_stm() {
    if (!_rm_stm) {
        vlog(
          clusterlog.error,
          "Topic {} doesn't support idempotent and transactional "
          "processing.",
          _raft->ntp());
    }
    return _rm_stm;
}

namespace {
template<class Units, class StagesFutureFunc>
ss::future<result<kafka_result>> stages_with_units_helper(
  ss::future<result<Units>> maybe_units_f,
  ss::promise<> enqueued_promise,
  StagesFutureFunc stages_future_func) {
    auto maybe_units = co_await std::move(maybe_units_f);
    if (!maybe_units.has_value()) {
        enqueued_promise.set_value();
        co_return maybe_units.error();
    }
    kafka_stages orig_stages = stages_future_func();
    co_await std::move(orig_stages.request_enqueued);
    enqueued_promise.set_value();
    co_return co_await std::move(orig_stages.replicate_finished);
}

template<class Units, class StagesFutureFunc>
kafka_stages stages_with_units(
  ss::future<result<Units>> maybe_units_f,
  StagesFutureFunc stages_future_func) {
    ss::promise<> enqueued_promise;
    auto enqueued_f = enqueued_promise.get_future();
    auto replicated_f = stages_with_units_helper(
      std::move(maybe_units_f),
      std::move(enqueued_promise),
      std::move(stages_future_func));
    return {std::move(enqueued_f), std::move(replicated_f)};
}
} // namespace

kafka_stages partition::replicate_in_stages(
  model::batch_identity bid,
  model::record_batch batch,
  raft::replicate_options opts) {
    using ret_t = result<kafka_result>;

    if (bid.is_transactional) {
        if (!_rm_stm) {
            vlog(
              clusterlog.error,
              "Topic {} doesn't support transactional processing.",
              _raft->ntp());
            return kafka_stages(raft::errc::timeout);
        }
    }

    if (bid.is_idempotent()) {
        if (!_rm_stm) {
            vlog(
              clusterlog.error,
              "Topic {} doesn't support idempotent requests.",
              _raft->ntp());
            return kafka_stages(raft::errc::timeout);
        }
    }

    return stages_with_units(
      hold_writes_enabled(),
      [this,
       bid = std::move(bid),
       batch = std::move(batch),
       opts = std::move(opts)]() mutable {
          if (_rm_stm) {
              return _rm_stm->replicate_in_stages(bid, std::move(batch), opts);
          }
          auto res = _raft->replicate_in_stages(std::move(batch), opts);
          auto replicate_finished = res.replicate_finished.then(
            [this](result<raft::replicate_result> r) {
                if (!r) {
                    return ret_t(r.error());
                }
                auto old_offset = r.value().last_offset;
                auto term = r.value().last_term;
                auto new_offset = kafka::offset(
                  log()->from_log_offset(old_offset)());
                return ret_t(kafka_result{new_offset, term});
            });
          return kafka_stages(
            std::move(res.request_enqueued), std::move(replicate_finished));
      });
}

raft::group_id partition::group() const { return _raft->group(); }

ss::future<> partition::start(
  raft::state_machine_manager_builder&& stm_builder,
  std::optional<xshard_transfer_state>&& xst_state) {
    const auto& ntp = _raft->ntp();

    std::optional<raft::xshard_transfer_state> raft_xst_state;
    if (xst_state) {
        raft_xst_state = xst_state->raft;
    }
    co_await _raft->start(std::move(stm_builder), std::move(raft_xst_state));
    // store rm_stm pointer in partition as this is commonly used stm
    _rm_stm = _raft->stm_manager()->get<cluster::rm_stm>();
    _log_eviction_stm = _raft->stm_manager()->get<cluster::log_eviction_stm>();
    // store partition properties stm offset for fast access
    _partition_properties_stm
      = _raft->stm_manager()->get<cluster::partition_properties_stm>();

    // Start the probe after the partition is fully initialised
    _probe.setup_metrics(ntp);
}

ss::future<> partition::stop() {
    auto partition_ntp = ntp();
    vlog(clusterlog.debug, "Stopping partition: {}", partition_ntp);
    _as.request_abort();

    _probe.clear_metrics();
    vlog(clusterlog.debug, "Stopped partition {}", partition_ntp);
    co_return;
}

ss::future<std::optional<storage::timequery_result>>
partition::timequery(storage::timequery_config cfg) {
    if (cfg.min_offset > cfg.max_offset) {
        co_return std::nullopt;
    }

    auto local_start_offset = log()->from_log_offset(_raft->start_offset());
    auto local_query_cfg = cfg;
    local_query_cfg.min_offset = std::max(
      local_start_offset, local_query_cfg.min_offset);

    co_return co_await local_timequery(local_query_cfg);
}

ss::future<std::optional<storage::timequery_result>>
partition::local_timequery(storage::timequery_config cfg) {
    vlog(clusterlog.debug, "timequery (raft) {} cfg(k)={}", _raft->ntp(), cfg);

    // Test before translation to handle empty log cases where
    // max_offset is before min_offset = start_offset.
    if (cfg.min_offset > cfg.max_offset) {
        co_return std::nullopt;
    }

    cfg.min_offset = _raft->log()->to_log_offset(cfg.min_offset);
    cfg.max_offset = _raft->log()->to_log_offset(cfg.max_offset);

    vlog(clusterlog.debug, "timequery (raft) {} cfg(r)={}", _raft->ntp(), cfg);

    auto result = co_await _raft->timequery(cfg);

    if (result.has_value()) {
        vlog(
          clusterlog.debug,
          "timequery (raft) {} cfg(r)={} result(r)={}",
          _raft->ntp(),
          cfg,
          result->offset);
        result->offset = _raft->log()->from_log_offset(result->offset);
    }

    co_return result;
}

uint64_t partition::non_log_disk_size_bytes() const {
    uint64_t raft_size = _raft->get_snapshot_size();
    uint64_t stm_local_size = 0;
    _raft->stm_manager()->for_each_stm(
      [this, &stm_local_size](
        const ss::sstring& name, const raft::state_machine_base& stm) {
          const auto sz = stm.get_local_state_size();
          vlog(
            clusterlog.trace,
            "local non-log disk size of {} stm {} = {} bytes",
            _raft->ntp(),
            name,
            sz);
          stm_local_size += sz;
      });

    vlog(
      clusterlog.trace,
      "local non-log disk size of {}: {}",
      _raft->ntp(),
      raft_size + stm_local_size);

    return raft_size + stm_local_size;
}

ss::future<> partition::update_configuration(topic_properties new_properties) {
    _raft->log()->set_overrides(new_properties.get_ntp_cfg_overrides());
    _raft->log()->notify_compaction_update();

    if (_topic_cfg) {
        _topic_cfg->properties = std::move(new_properties);
    }

    _raft->notify_config_update();
    co_return;
}

std::optional<model::offset>
partition::get_term_last_offset(model::term_id term) const {
    auto o = _raft->log()->get_term_last_offset(term);
    if (!o) {
        return std::nullopt;
    }
    // Kafka defines leader epoch last offset as a first offset of next
    // leader epoch
    return model::next_offset(*o);
}

ss::future<> partition::remove_persistent_state() {
    co_await _raft->stm_manager()->remove_local_state();
}

void partition::set_topic_config(
  std::unique_ptr<cluster::topic_configuration> cfg) {
    _topic_cfg = std::move(cfg);
}

ss::future<std::error_code>
partition::transfer_leadership(raft::transfer_leadership_request req) {
    auto target = req.target;

    vlog(
      clusterlog.debug,
      "Transferring {} leadership to {}",
      ntp(),
      target.value_or(model::node_id{-1}));

    // Some state machines need a preparatory phase to efficiently transfer
    // leadership: invoke this, and hold the lock that they return until
    // the leadership transfer attempt is complete.
    ss::rwlock::holder stm_prepare_lock;
    if (_rm_stm) {
        stm_prepare_lock = co_await _rm_stm->prepare_transfer_leadership();
    } else if (auto stm = tm_stm(); stm) {
        stm_prepare_lock = co_await stm->prepare_transfer_leadership();
    }

    co_return co_await _raft->do_transfer_leadership(req);
}

result<std::vector<raft::follower_metrics>>
partition::get_follower_metrics() const {
    if (!_raft->is_leader()) {
        return errc::not_leader;
    };
    return _raft->get_follower_metrics();
}

ss::future<result<model::offset, std::error_code>>
partition::sync_kafka_start_offset_override(
  model::timeout_clock::duration timeout) {
    try {
        if (_log_eviction_stm) {
            auto offset_res = co_await _log_eviction_stm
                                ->sync_kafka_start_offset_override(timeout);
            if (offset_res.has_failure()) {
                co_return offset_res.as_failure();
            }
            if (offset_res.value() != kafka::offset{}) {
                co_return kafka::offset_cast(offset_res.value());
            }
        }

        co_return model::offset{};
    } catch (...) {
        auto eptr = std::current_exception();
        bool is_shutdown = ssx::is_shutdown_exception(eptr);
        vlogl(
          clusterlog,
          is_shutdown ? ss::log_level::debug : ss::log_level::warn,
          "ntp {}: exception in sync_kafka_start_offset_override: {}",
          _raft->ntp(),
          eptr);
        if (is_shutdown) {
            co_return errc::shutting_down;
        }
        co_return errc::timeout;
    }
}

model::offset partition::last_stable_offset() const {
    if (_rm_stm) {
        return _rm_stm->last_stable_offset();
    }

    return high_watermark();
}

std::optional<model::offset> partition::eviction_requested_offset() {
    if (_log_eviction_stm) {
        return _log_eviction_stm->eviction_requested_offset();
    } else {
        return std::nullopt;
    }
}

ss::shared_ptr<cluster::id_allocator_stm> partition::id_allocator_stm() const {
    return _raft->stm_manager()->get<cluster::id_allocator_stm>();
}

fmt::iterator partition::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{}", *_raft);
}
ss::shared_ptr<cluster::tm_stm> partition::tm_stm() {
    return _raft->stm_manager()->get<cluster::tm_stm>();
}

ss::future<chunked_vector<tx::tx_range>>
partition::aborted_transactions(model::offset from, model::offset to) {
    if (!_rm_stm) {
        return ss::make_ready_future<chunked_vector<tx::tx_range>>(
          chunked_vector<tx::tx_range>());
    }
    return _rm_stm->aborted_transactions(from, to);
}

model::producer_id partition::highest_producer_id() {
    return _rm_stm ? _rm_stm->highest_producer_id() : model::producer_id{};
}

model::offset partition::max_removable_local_log_offset() {
    return _raft->log()->stm_hookset()->max_removable_local_log_offset();
}

std::optional<model::offset> partition::kafka_start_offset_override() const {
    if (_log_eviction_stm) {
        auto o = _log_eviction_stm->kafka_start_offset_override();
        if (o != kafka::offset{}) {
            return kafka::offset_cast(o);
        }
    }
    return std::nullopt;
}

std::optional<std::reference_wrapper<cluster::topic_configuration>>
partition::get_topic_config() {
    if (_topic_cfg) {
        return std::ref(*_topic_cfg);
    } else {
        return std::nullopt;
    }
}

ss::sharded<features::feature_table>& partition::feature_table() const {
    return _feature_table;
}

ss::future<model::record_batch_reader>
partition::make_local_reader(storage::local_log_reader_config config) {
    return _raft->make_reader(config);
}

model::term_id partition::term() const { return _raft->term(); }

model::offset partition::raft_start_offset() const {
    return _raft->start_offset();
}

model::offset partition::committed_offset() const {
    return _raft->committed_offset();
}
model::offset partition::high_watermark() const {
    auto lsi = _raft->last_visible_index();
    auto start_offset = _raft->start_offset();
    if (lsi < start_offset) {
        // Doesn't need next_offset since start_offset is already
        // the first offset that can be produced to.
        return start_offset;
    }
    return model::next_offset(lsi);
}
model::offset partition::leader_high_watermark() const {
    return model::next_offset(_raft->last_leader_visible_index());
}
model::offset partition::dirty_offset() const {
    return _raft->log()->offsets().dirty_offset;
}

const model::ntp& partition::ntp() const { return _raft->ntp(); }

ss::shared_ptr<storage::log> partition::log() const { return _raft->log(); }

bool partition::is_elected_leader() const { return _raft->is_elected_leader(); }
bool partition::is_leader() const { return _raft->is_leader(); }
bool partition::has_followers() const { return _raft->has_followers(); }
void partition::block_new_leadership() const {
    return _raft->block_new_leadership();
}
void partition::unblock_new_leadership() const {
    return _raft->unblock_new_leadership();
}

ss::future<result<model::offset>> partition::linearizable_barrier() {
    return _raft->linearizable_barrier();
}

ss::future<std::error_code> partition::update_replica_set(
  std::vector<raft::broker_revision> brokers,
  model::revision_id new_revision_id) {
    return _raft->replace_configuration(std::move(brokers), new_revision_id);
}

ss::future<std::error_code> partition::update_replica_set(
  std::vector<raft::vnode> nodes,
  model::revision_id new_revision_id,
  std::optional<model::offset> learner_start_offset) {
    return _raft->replace_configuration(
      std::move(nodes), new_revision_id, learner_start_offset);
}

ss::future<std::error_code> partition::force_update_replica_set(
  std::vector<raft::vnode> voters,
  std::vector<raft::vnode> learners,
  model::revision_id new_revision_id) {
    return _raft->force_replace_configuration_locally(
      std::move(voters), std::move(learners), new_revision_id);
}

raft::group_configuration partition::group_configuration() const {
    return _raft->config();
}

model::revision_id partition::get_revision_id() const {
    return _raft->config().revision_id();
}
model::revision_id partition::get_log_revision_id() const {
    return _raft->log_config().get_revision();
}

model::revision_id partition::get_topic_revision_id() const {
    return _raft->log_config().get_topic_revision();
}

std::optional<model::node_id> partition::get_leader_id() const {
    return _raft->get_leader_id();
}

std::optional<uint8_t> partition::get_under_replicated() const {
    return _raft->get_under_replicated();
}

model::offset partition::get_latest_configuration_offset() const {
    return _raft->get_latest_configuration_offset();
}

ss::lw_shared_ptr<const storage::offset_translator_state>
partition::get_offset_translator_state() const {
    return _raft->log()->get_offset_translator_state();
}

size_t partition::size_bytes() const { return _raft->log()->size_bytes(); }
size_t partition::reclaimable_size_bytes() const {
    return _raft->log()->reclaimable_size_bytes();
}

const storage::ntp_config& partition::get_ntp_config() const {
    return _raft->log_config();
}
model::term_id partition::get_term(model::offset o) const {
    return _raft->get_term(o);
}

ss::future<std::error_code>
partition::cancel_replica_set_update(model::revision_id rev) {
    return _raft->cancel_configuration_change(rev);
}

ss::future<std::error_code>
partition::force_abort_replica_set_update(model::revision_id rev) {
    return _raft->abort_configuration_change(rev);
}
consensus_ptr partition::raft() const { return _raft; }

ss::future<result<model::offset>> partition::set_writes_disabled(
  partition_properties_stm::writes_disabled disable,
  model::timeout_clock::time_point deadline,
  model::revision_id revision_id) {
    ss::rwlock::holder holder;
    auto lock_deadline = ss::semaphore::clock::now()
                         + ss::semaphore::clock::duration(
                           deadline - model::timeout_clock::now());
    try {
        holder = co_await _produce_lock.hold_write_lock(lock_deadline);
    } catch (ss::semaphore_timed_out&) {
        co_return errc::timeout;
    }
    if (!_feature_table.local().is_active(
          features::feature::partition_properties_stm)) {
        co_return errc::feature_disabled;
    }
    if (_partition_properties_stm == nullptr) {
        co_return errc::invalid_partition_operation;
    }

    // abort active transactions
    if (disable && _rm_stm) {
        auto res = co_await _rm_stm->abort_all_txes();
        if (res != tx::errc::none) {
            co_return res;
        }
    }

    auto method = disable ? &partition_properties_stm::disable_writes
                          : &partition_properties_stm::enable_writes;
    co_return co_await (*_partition_properties_stm.*method)(revision_id);
}

partition_flush_hook_id partition::register_flush_hook(flush_hook&& cb) {
    return _flush_hooks.register_cb(std::move(cb));
}

void partition::unregister_flush_hook(partition_flush_hook_id id) {
    _flush_hooks.unregister_cb(id);
}

ss::future<errc> partition::flush(
  model::offset offset,
  model::timeout_clock::time_point deadline,
  ss::abort_source& as) {
    // non-leader may lack some flush hooks
    if (!is_leader()) {
        co_return errc::not_leader;
    }

    vlog(clusterlog.info, "[{}] flushing offset {}", ntp(), offset);
    auto futures = _flush_hooks.notify(offset, deadline, as);
    using errs = std::vector<errc>;
    errs results = co_await ssx::when_all_succeed<errs>(std::move(futures));
    vlog(clusterlog.info, "[{}] flushed offset {}", ntp(), offset);
    co_return *std::ranges::max_element(results);
}

ss::future<result<ss::rwlock::holder>> partition::hold_writes_enabled() {
    auto maybe_units = _produce_lock.try_hold_read_lock();
    if (!maybe_units) {
        co_return errc::resource_is_being_migrated;
    }

    auto are_disabled
      = _partition_properties_stm
          ? co_await _partition_properties_stm->sync_writes_disabled()
          : partition_properties_stm::writes_disabled::no;
    if (!are_disabled.has_value()) {
        co_return are_disabled.error();
    }
    if (are_disabled.value()) {
        co_return errc::resource_is_being_migrated;
    }

    co_return *std::move(maybe_units);
}

} // namespace cluster

namespace seastar {

void lw_shared_ptr_deleter<cluster::partition>::dispose(cluster::partition* s) {
    delete s;
}

} // namespace seastar
