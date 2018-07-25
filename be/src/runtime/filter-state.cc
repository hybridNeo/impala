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
