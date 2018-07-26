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

#include <runtime/filter-state.h>

#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_constants.h"

#include "util/bloom-filter.h"
#include "util/min-max-filter.h"

using namespace impala;

void FilterState::ApplyUpdate(
    const TUpdateFilterParams& params, MemTracker* filter_mem_tracker) {
  DCHECK(!disabled());
  DCHECK_GT(pending_count_, 0);
  DCHECK_EQ(completion_time_, 0L);
  if (first_arrival_time_ == 0L) {
    // TODO first_arrival_time_ = coord->query_events_->ElapsedTime();
  }

  --pending_count_;
  if (is_bloom_filter()) {
    DCHECK(params.__isset.bloom_filter);
    if (params.bloom_filter.always_true) {
      Disable(filter_mem_tracker);
    } else if (bloom_filter_.always_false) {
      int64_t heap_space = params.bloom_filter.directory.size();
      if (!filter_mem_tracker->TryConsume(heap_space)) {
        VLOG_QUERY << "Not enough memory to allocate filter: "
                   << PrettyPrinter::Print(heap_space, TUnit::BYTES)
                   << " (query_id=" /*TODO << PrintId(coord->query_id())  */ << ")";
        // Disable, as one missing update means a correct filter cannot be produced.
        Disable(filter_mem_tracker);
      } else {
        // Workaround for fact that parameters are const& for Thrift RPCs - yet we want to
        // move the payload from the request rather than copy it and take double the
        // memory cost. After this point, params.bloom_filter is an empty filter and
        // should not be read.
        TBloomFilter* non_const_filter = &const_cast<TBloomFilter&>(params.bloom_filter);
        swap(bloom_filter_, *non_const_filter);
        DCHECK_EQ(non_const_filter->directory.size(), 0);
      }
    } else {
      BloomFilter::Or(params.bloom_filter, &bloom_filter_);
    }
  } else {
    DCHECK(is_min_max_filter());
    DCHECK(params.__isset.min_max_filter);
    if (params.min_max_filter.always_true) {
      Disable(filter_mem_tracker);
    } else if (min_max_filter_.always_false) {
      MinMaxFilter::Copy(params.min_max_filter, &min_max_filter_);
    } else {
      MinMaxFilter::Or(params.min_max_filter, &min_max_filter_);
    }
  }

  if (pending_count_ == 0 || disabled()) {
    // TODO completion_time_ = coord->query_events_->ElapsedTime();
  }
}

bool FilterState::disabled() const {
  if (is_bloom_filter()) {
    return bloom_filter_.always_true;
  } else {
    DCHECK(is_min_max_filter());
    return min_max_filter_.always_true;
  }
}

void FilterState::ToThrift(TFilterState* f) const {
  std::vector<TFilterTarget> t_targets;
  for (const FilterTarget& filter_target : targets_) {
    TFilterTarget thrift_filter_target;
    filter_target.ToThrift(&thrift_filter_target);
    t_targets.push_back(thrift_filter_target);
  }
  f->__set_targets(t_targets);
  f->__set_desc(desc_);
  f->__set_src(src_);
  f->__set_pending_count(pending_count_);
  /// Need to cast the int type of this class to int32_t of thrift
  for (int32_t idx : src_fragment_instance_idxs_) {
    f->src_fragment_instance_idxs.insert(idx);
  }
  f->__set_bloom_filter(bloom_filter_);
  f->__set_min_max_filter(min_max_filter_);
  f->__set_first_arrival_time(first_arrival_time_);
  f->__set_completion_time(completion_time_);
  f->__set_aggregator_address(aggregator_address_);
}

FilterState FilterState::FromThrift(const TFilterState& f) {
  FilterState fs(f.desc, f.src, f.aggregator_address, f.pending_count,
      f.first_arrival_time, f.completion_time, f.bloom_filter, f.min_max_filter);
  std::vector<FilterTarget> targets;
  for (const TFilterTarget& filter_target : f.targets) {
    targets.push_back(FilterTarget::FromThrift(filter_target));
  }
  fs.set_targets(targets);
  std::unordered_set<int> src_fragment_instance_idxs;
  for (int idx : f.src_fragment_instance_idxs) {
    src_fragment_instance_idxs.insert(idx);
  }
  fs.set_src_fragment_instance_idxs(src_fragment_instance_idxs);

  return fs;
}


void FilterState::Disable(MemTracker* tracker) {
  if (is_bloom_filter()) {
    bloom_filter_.always_true = true;
    bloom_filter_.always_false = false;
    tracker->Release(bloom_filter_.directory.size());
    bloom_filter_.directory.clear();
    bloom_filter_.directory.shrink_to_fit();
  } else {
    DCHECK(is_min_max_filter());
    min_max_filter_.always_true = true;
    min_max_filter_.always_false = false;
  }
}
