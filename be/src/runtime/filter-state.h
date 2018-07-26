// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <memory>
#include <vector>
#include <unordered_set>
#include "common/status.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Types_types.h"

namespace impala {

class MemTracker;

/// Represents a runtime filter target.
struct FilterTarget {
  TPlanNodeId node_id;
  bool is_local;
  bool is_bound_by_partition_columns;
  int fragment_idx;

  FilterTarget(const TRuntimeFilterTargetDesc& desc, int f_idx)
    : node_id(desc.node_id),
      is_local(desc.is_local_target),
      is_bound_by_partition_columns(desc.is_bound_by_partition_columns),
      fragment_idx(f_idx) {}

  FilterTarget(const TPlanNodeId node_id, bool is_local,
      bool is_bound_by_partition_columns, int fragment_idx)
    : node_id(node_id),
      is_local(is_local),
      is_bound_by_partition_columns(is_bound_by_partition_columns),
      fragment_idx(fragment_idx) {}

  void ToThrift(TFilterTarget* t) const {
    t->__set_node_id(node_id);
    t->__set_is_local(is_local);
    t->__set_is_bound_by_partition_columns(is_bound_by_partition_columns);
    t->__set_fragment_idx(fragment_idx);
  }

  static FilterTarget FromThrift(const TFilterTarget& tfilter_target) {
    FilterTarget f(tfilter_target.node_id, tfilter_target.is_local,
        tfilter_target.is_bound_by_partition_columns, tfilter_target.fragment_idx);
    return f;
  }
};

/// State of runtime filters that are received for aggregation. A runtime filter will
/// contain a bloom or min-max filter.
///
/// A broadcast join filter is published as soon as the first update is received for it
/// and subsequent updates are ignored (as they will be the same).
/// Updates for a partitioned join filter are aggregated and then published once
/// 'pending_count' reaches 0 and if the filter was not disabled before that.
///
///
/// A filter is disabled if an always_true filter update is received, an OOM is hit,
/// filter aggregation is complete or if the query is complete.
/// Once a filter is disabled, subsequent updates for that filter are ignored.
class FilterState {
 public:
  FilterState(const TRuntimeFilterDesc& desc, const TPlanNodeId& src)
    : desc_(desc),
      src_(src),
      pending_count_(0),
      first_arrival_time_(0L),
      completion_time_(0L) {
    // bloom_filter_ is a disjunction so the unit value is always_false.
    bloom_filter_.always_false = true;
    min_max_filter_.always_false = true;
  }

  FilterState(const TRuntimeFilterDesc& desc, const TPlanNodeId& src,
      const TNetworkAddress& aggregator_address, int pending_count,
      int64_t first_arrival_time, int64_t completion_time,
      const TBloomFilter& bloom_filter, const TMinMaxFilter& min_max_filter)
    : desc_(desc),
      src_(src),
      aggregator_address_(aggregator_address),
      pending_count_(pending_count),
      bloom_filter_(bloom_filter),
      min_max_filter_(min_max_filter),
      first_arrival_time_(first_arrival_time),
      completion_time_(completion_time) {}

  TBloomFilter& bloom_filter() { return bloom_filter_; }
  TMinMaxFilter& min_max_filter() { return min_max_filter_; }
  std::unordered_set<int>* src_fragment_instance_idxs() {
    return &src_fragment_instance_idxs_;
  }
  TNetworkAddress aggregator_address() { return aggregator_address_; }
  const std::unordered_set<int>& src_fragment_instance_idxs() const {
    return src_fragment_instance_idxs_;
  }

  void set_src_fragment_instance_idxs(const std::unordered_set<int>& set) {
    src_fragment_instance_idxs_ = set;
  }
  void set_targets(const std::vector<FilterTarget>& targets) { targets_ = targets; }
  std::vector<FilterTarget>* targets() { return &targets_; }
  const std::vector<FilterTarget>& targets() const { return targets_; }
  int64_t first_arrival_time() const { return first_arrival_time_; }
  int64_t completion_time() const { return completion_time_; }
  const TPlanNodeId& src() const { return src_; }
  const TRuntimeFilterDesc& desc() const { return desc_; }
  bool is_bloom_filter() const { return desc_.type == TRuntimeFilterType::BLOOM; }
  bool is_min_max_filter() const { return desc_.type == TRuntimeFilterType::MIN_MAX; }
  int pending_count() const { return pending_count_; }
  void set_pending_count(int pending_count) { pending_count_ = pending_count; }
  void set_aggregator_address(TNetworkAddress aggregator_address) {
    aggregator_address_ = aggregator_address;
  }

  bool disabled() const;

  void ToThrift(TFilterState* f) const;

  static FilterState FromThrift(const TFilterState& f);

  /// Aggregates partitioned join filters and updates memory consumption.
  /// Disables filter if always_true filter is received or OOM is hit.
  void ApplyUpdate(const TUpdateFilterParams& params, MemTracker* filter_mem_tracker);

  /// Disables a filter. A disabled filter consumes no memory.
  void Disable(MemTracker* tracker);

 private:
  /// Contains the specification of the runtime filter.
  TRuntimeFilterDesc desc_;

  TPlanNodeId src_;
  std::vector<FilterTarget> targets_;

  // Network Address of the aggregator for the particular filter
  TNetworkAddress aggregator_address_;

  // Indices of source fragment instances (as returned by GetInstanceIdx()).
  std::unordered_set<int> src_fragment_instance_idxs_;

  /// Number of remaining backends to hear from before filter is complete.
  int pending_count_;

  /// Filters aggregated from all source plan nodes, to be broadcast to all
  /// destination plan fragment instances. Only set for partitioned joins (broadcast joins
  /// need no aggregation).
  /// In order to avoid memory spikes, an incoming filter is moved (vs. copied) to the
  /// output structure in the case of a broadcast join. Similarly, for partitioned joins,
  /// the filter is moved from the following member to the output structure.
  TBloomFilter bloom_filter_;
  TMinMaxFilter min_max_filter_;

  /// Time at which first local filter arrived.
  int64_t first_arrival_time_;

  /// Time at which all local filters arrived.
  int64_t completion_time_;

  /// TODO: Add a per-object lock so that we can avoid holding the global filter_lock_
  /// for every filter update.
};

} // namespace impala
