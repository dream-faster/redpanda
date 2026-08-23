/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/format_to.h"
#include "cluster/fwd.h"
#include "cluster/partition_probe.h"
#include "cluster/partition_properties_stm.h"
#include "cluster/types.h"
#include "features/fwd.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/replicate.h"
#include "storage/ntp_config.h"
#include "storage/translating_reader.h"
#include "storage/types.h"
#include "utils/notification_list.h"

#include <seastar/core/shared_ptr.hh>

namespace cluster {
class partition_manager;

// A struct holding in-memory state that can make starting the partition
// instance on the destination shard of the x-shard transfer easier. Note that
// it is strictly an optimization, as the partition must always be able to
// perform a "cold start" from persistent state only.
struct xshard_transfer_state {
    raft::xshard_transfer_state raft;
};

/// holds cluster logic that is not raft related
/// all raft logic is proxied transparently
class partition : public ss::enable_lw_shared_from_this<partition> {
public:
    partition(consensus_ptr r, ss::sharded<features::feature_table>&);

    ~partition() = default;

    raft::group_id group() const;
    ss::future<> start(
      raft::state_machine_manager_builder&&,
      std::optional<xshard_transfer_state>&&);
    ss::future<> stop();

    ss::future<result<kafka_result>> replicate(
      chunked_vector<model::record_batch> batches, raft::replicate_options);

    /// Truncate the beginning of the log up until a given offset
    /// Can only be performed on logs that are deletable and non internal
    ss::future<std::error_code> prefix_truncate(
      model::offset o, kafka::offset ko, ss::lowres_clock::time_point deadline);

    kafka_stages replicate_in_stages(
      model::batch_identity,
      model::record_batch batch,
      raft::replicate_options);

    /**
     * The reader is modified such that the max offset is configured to be
     * the minimum of the max offset requested and the committed index of the
     * underlying raft group.
     */
    ss::future<model::record_batch_reader>
    make_local_reader(storage::local_log_reader_config config);

    ss::future<result<model::offset, std::error_code>>
    sync_kafka_start_offset_override(model::timeout_clock::duration timeout);

    model::offset raft_start_offset() const;

    /**
     * The returned value of last committed offset should not be used to
     * do things like initialize a reader (use partition::make_reader). Instead
     * it can be used to report upper offset bounds to clients.
     */
    model::offset committed_offset() const;

    /**
     * <kafka>The last stable offset (LSO) is defined as the first offset such
     * that all lower offsets have been "decided." Non-transactional messages
     * are considered decided immediately, but transactional messages are only
     * decided when the corresponding COMMIT or ABORT marker is written. This
     * implies that the last stable offset will be equal to the high watermark
     * if there are no transactional messages in the log. Note also that the LSO
     * cannot advance beyond the high watermark.  </kafka>
     *
     * There are two important pieces in this comment:
     *
     *   1) "non-transaction message are considered decided immediately".
     *   Since we currently use the commited_offset to report the end of log to
     *   kafka clients, simply report the next offset.
     *
     *   2) "first offset such that all lower offsets have been decided". this
     *   is describing a strictly greater than relationship.
     */
    model::offset last_stable_offset() const;
    /**
     * All batches with offets smaller than high watermark are visible to
     * consumers. Named high_watermark to be consistent with Kafka nomenclature.
     */
    model::offset high_watermark() const;

    model::offset leader_high_watermark() const;

    model::term_id term() const;

    model::offset dirty_offset() const;

    /// Return the offset up to which the storage layer would like to
    /// prefix truncate the log, if any.  This may be consumed as an indicator
    /// that any truncation-delaying activitiy (like uploading to tiered
    /// storage) could be expedited to enable local disk space to be reclaimed.
    std::optional<model::offset> eviction_requested_offset();

    const model::ntp& ntp() const;

    ss::shared_ptr<storage::log> log() const;

    ss::future<std::optional<storage::timequery_result>>
      timequery(storage::timequery_config);

    bool is_elected_leader() const;
    bool is_leader() const;
    bool has_followers() const;
    void block_new_leadership() const;
    void unblock_new_leadership() const;

    ss::future<result<model::offset>> linearizable_barrier();

    ss::future<std::error_code>
      transfer_leadership(raft::transfer_leadership_request);

    /**
     * Returns the maximum offset that may not be delivered to the newly joining
     * learners as claimed by the state machines implemented on top of this
     * partition.
     */
    model::offset max_removable_local_log_offset();

    ss::future<std::error_code> update_replica_set(
      std::vector<raft::broker_revision> brokers,
      model::revision_id new_revision_id);

    ss::future<std::error_code> update_replica_set(
      std::vector<raft::vnode> nodes,
      model::revision_id new_revision_id,
      std::optional<model::offset> learner_start_offset);

    ss::future<std::error_code> force_update_replica_set(
      std::vector<raft::vnode> voters,
      std::vector<raft::vnode> learners,
      model::revision_id new_revision_id);

    raft::group_configuration group_configuration() const;
    partition_probe& probe() { return _probe; }

    model::revision_id get_revision_id() const;
    model::revision_id get_log_revision_id() const;
    model::revision_id get_topic_revision_id() const;

    std::optional<model::node_id> get_leader_id() const;

    std::optional<uint8_t> get_under_replicated() const;

    model::offset get_latest_configuration_offset() const;

    ss::shared_ptr<cluster::id_allocator_stm> id_allocator_stm() const;

    ss::lw_shared_ptr<const storage::offset_translator_state>
    get_offset_translator_state() const;

    ss::shared_ptr<cluster::rm_stm> rm_stm();

    size_t size_bytes() const;

    size_t reclaimable_size_bytes() const;

    uint64_t non_log_disk_size_bytes() const;

    ss::future<> update_configuration(topic_properties);

    const storage::ntp_config& get_ntp_config() const;
    ss::shared_ptr<cluster::tm_stm> tm_stm();

    ss::future<chunked_vector<model::tx_range>>
    aborted_transactions(model::offset from, model::offset to);

    model::producer_id highest_producer_id();

    std::optional<model::offset> kafka_start_offset_override() const;

    ss::future<> remove_persistent_state();
    std::optional<model::offset> get_term_last_offset(model::term_id) const;

    model::term_id get_term(model::offset o) const;
    ss::future<std::error_code>
    cancel_replica_set_update(model::revision_id rev);

    ss::future<std::error_code>
    force_abort_replica_set_update(model::revision_id rev);

    consensus_ptr raft() const;

    /**
     * Partition 0 carries a copy of the topic configuration, updated by
     * the controller.
     */
    void set_topic_config(std::unique_ptr<cluster::topic_configuration> cfg);

    std::optional<std::reference_wrapper<cluster::topic_configuration>>
    get_topic_config();

    ss::sharded<features::feature_table>& feature_table() const;

    result<std::vector<raft::follower_metrics>> get_follower_metrics() const;
    ss::future<result<model::offset>> set_writes_disabled(
      partition_properties_stm::writes_disabled disable,
      model::timeout_clock::time_point deadline,
      model::revision_id revision_id);

    using flush_hook = ss::noncopyable_function<ss::future<errc>(
      model::offset,
      model::timeout_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>>)>;

    // Register and execute actions to make sure we leave partition belongings
    // in up-to-date state. Used for unmount.
    partition_flush_hook_id register_flush_hook(flush_hook&& cb);
    void unregister_flush_hook(partition_flush_hook_id id);
    ss::future<errc>
    flush(model::offset, model::timeout_clock::time_point, ss::abort_source&);

    bool started() const noexcept { return _started; }
    void mark_started() noexcept { _started = true; }

    // Acquire a shared lock for producing to the partition.
    ss::future<result<ss::rwlock::holder>> hold_writes_enabled();

    fmt::iterator format_to(fmt::iterator it) const;

    ss::future<std::optional<storage::timequery_result>>
      local_timequery(storage::timequery_config);

    consensus_ptr _raft; // never null
    ss::shared_ptr<cluster::log_eviction_stm> _log_eviction_stm;
    ss::shared_ptr<cluster::rm_stm> _rm_stm;
    ss::shared_ptr<partition_properties_stm> _partition_properties_stm;
    ss::abort_source _as;
    partition_probe _probe;
    ss::sharded<features::feature_table>& _feature_table;
    // Populated for partition 0 only, used to generate topic manifests.
    std::unique_ptr<cluster::topic_configuration> _topic_cfg;

    config::binding<model::cleanup_policy_bitflags> _log_cleanup_policy;

    // acquire shared ("read") for produce,
    // exclusive ("write") for enabling/disabling writes
    ss::rwlock _produce_lock;

    notification_list<flush_hook, partition_flush_hook_id> _flush_hooks;

    bool _started{false};
};
} // namespace cluster
namespace std {
template<>
struct hash<cluster::partition> {
    size_t operator()(const cluster::partition& x) const {
        return std::hash<model::ntp>()(x.ntp());
    }
};
} // namespace std
