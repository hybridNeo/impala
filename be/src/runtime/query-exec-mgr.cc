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


#include "runtime/query-exec-mgr.h"

#include <gperftools/malloc_extension.h>
#include <gutil/strings/substitute.h>
#include <boost/thread/locks.hpp>
#include <boost/thread/lock_guard.hpp>

#include "common/logging.h"
#include "runtime/query-state.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "util/uid-util.h"
#include "util/thread.h"
#include "util/impalad-metrics.h"
#include "util/debug-util.h"

#include "common/names.h"

using namespace impala;

void QueryExecMgr::FilterAggregator::InitFilterRoutingTable(const TExecQueryFInstancesParams& rpc_params) {
  int num_instances = 3;
  vector<int> src_idxs;
  for (const TPlanNode& plan_node: rpc_params.fragment_ctxs.back().fragment.plan.nodes) {
    if (!plan_node.__isset.runtime_filters) continue; 
    for (const TRuntimeFilterDesc& filter: plan_node.runtime_filters) {
      VLOG_QUERY << "RUNTIME_FILTERING : filter_id : " << filter.filter_id;  
      FilterRoutingTable::iterator i = filter_routing_table_.emplace(
          filter.filter_id, FilterState(filter, plan_node.node_id)).first;
      FilterState* f = &(i->second);   
      // source plan node of filter
      if (plan_node.__isset.hash_join_node) {
        // Set the 'pending_count_' to zero to indicate that for a filter with
        // local-only targets the coordinator does not expect to receive any filter
        // updates.
        int pending_count = filter.is_broadcast_join
            ? (filter.has_remote_targets ? 1 : 0) : num_instances;
        f->set_pending_count(pending_count);

        // determine source instances
        // TODO: store this in FInstanceExecParams, not in FilterState
        //vector<int> src_idxs = fragment_params.GetInstanceIdxs();

        // If this is a broadcast join with only non-local targets, build and publish it
        // on MAX_BROADCAST_FILTER_PRODUCERS instances. If this is not a broadcast join
        // or it is a broadcast join with local targets, it should be generated
        // everywhere the join is executed.
        // if (filter.is_broadcast_join && !filter.has_local_targets
        //     && num_instances > MAX_BROADCAST_FILTER_PRODUCERS) {
        //   random_shuffle(src_idxs.begin(), src_idxs.end());
        //   src_idxs.resize(MAX_BROADCAST_FILTER_PRODUCERS);
        // }
        // f->src_fragment_instance_idxs()->insert(src_idxs.begin(), src_idxs.end());

      // target plan node of filter
      } else if (plan_node.__isset.hdfs_scan_node || plan_node.__isset.kudu_scan_node) {
        // auto it = filter.planid_to_target_ndx.find(plan_node.node_id);
        // DCHECK(it != filter.planid_to_target_ndx.end());
        // const TRuntimeFilterTargetDesc& t_target = filter.targets[it->second];
        // DCHECK(filter_mode_ == TRuntimeFilterMode::GLOBAL || t_target.is_local_target);
        // f->targets()->emplace_back(t_target, fragment_params.fragment.idx);
      } else {
        // DCHECK(false) << "Unexpected plan node with runtime filters: "
        //     << ThriftDebugString(plan_node);
      }
      map<int32_t, TNetworkAddress>::const_iterator it =
          rpc_params.aggregator_routing_table.find(filter.filter_id);
      if (it != rpc_params.aggregator_routing_table.end()) {
        VLOG_QUERY << "Node address " 
            << TNetworkAddressToString(it->second); 
       }
     } 
  }
}

// TODO: this logging should go into a per query log.
DEFINE_int32(log_mem_usage_interval, 0, "If non-zero, impalad will output memory usage "
    "every log_mem_usage_interval'th fragment completion.");

Status QueryExecMgr::StartQuery(const TExecQueryFInstancesParams& params) {
  TUniqueId query_id = params.query_ctx.query_id;
  VLOG_QUERY << "StartQueryFInstances() query_id=" << PrintId(query_id)
             << " coord=" << TNetworkAddressToString(params.query_ctx.coord_address);
  VLOG_QUERY << "Size of real : Transferred : " << params.aggregator_routing_table.size(); 
  VLOG_QUERY << "Printing out aggregator parameters";
  map<int32_t, TNetworkAddress>::const_iterator it;
  for (it = params.aggregator_routing_table.begin(); it != 
          params.aggregator_routing_table.end(); it++) {
    VLOG_QUERY <<  std::to_string(it->first) << TNetworkAddressToString(it->second) ;
  }
  
  filter_aggregator_.InitFilterRoutingTable(params);
  bool dummy;
  QueryState* qs = GetOrCreateQueryState(params.query_ctx, &dummy);
  Status status = qs->Init(params);
  if (!status.ok()) {
    qs->ReleaseExecResourceRefcount(); // Release refcnt acquired in Init().
    ReleaseQueryState(qs);
    return status;
  }
  // avoid blocking the rpc handler thread for too long by starting a new thread for
  // query startup (which takes ownership of the QueryState reference)
  unique_ptr<Thread> t;
  status = Thread::Create("query-exec-mgr",
      Substitute("start-query-finstances-$0", PrintId(query_id)),
          &QueryExecMgr::StartQueryHelper, this, qs, &t, true);
  if (!status.ok()) {
    // decrement refcount taken in QueryState::Init()
    qs->ReleaseExecResourceRefcount();
    // decrement refcount taken in GetOrCreateQueryState()
    ReleaseQueryState(qs);
    return status;
  }
  t->Detach();
  return Status::OK();
}

QueryState* QueryExecMgr::CreateQueryState(const TQueryCtx& query_ctx) {
  bool created;
  QueryState* qs = GetOrCreateQueryState(query_ctx, &created);
  DCHECK(created);
  return qs;
}

QueryState* QueryExecMgr::GetQueryState(const TUniqueId& query_id) {
  QueryState* qs = nullptr;
  int refcnt;
  {
    ScopedShardedMapRef<QueryState*> map_ref(query_id,
        &ExecEnv::GetInstance()->query_exec_mgr()->qs_map_);
    DCHECK(map_ref.get() != nullptr);

    auto it = map_ref->find(query_id);
    if (it == map_ref->end()) return nullptr;
    qs = it->second;
    refcnt = qs->refcnt_.Add(1);
  }
  DCHECK(qs != nullptr && refcnt > 0);
  VLOG_QUERY << "QueryState: query_id=" << PrintId(query_id) << " refcnt=" << refcnt;
  return qs;
}

QueryState* QueryExecMgr::GetOrCreateQueryState(
    const TQueryCtx& query_ctx, bool* created) {
  QueryState* qs = nullptr;
  int refcnt;
  {
    ScopedShardedMapRef<QueryState*> map_ref(query_ctx.query_id,
        &ExecEnv::GetInstance()->query_exec_mgr()->qs_map_);
    DCHECK(map_ref.get() != nullptr);

    auto it = map_ref->find(query_ctx.query_id);
    if (it == map_ref->end()) {
      // register new QueryState
      qs = new QueryState(query_ctx);
      map_ref->insert(make_pair(query_ctx.query_id, qs));
      *created = true;
    } else {
      qs = it->second;
      *created = false;
    }
    // decremented by ReleaseQueryState()
    refcnt = qs->refcnt_.Add(1);
  }
  DCHECK(qs != nullptr && refcnt > 0);
  return qs;
}


void QueryExecMgr::StartQueryHelper(QueryState* qs) {
  qs->StartFInstances();

#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  // tcmalloc and address or thread sanitizer cannot be used together
  if (FLAGS_log_mem_usage_interval > 0) {
    uint64_t num_complete = ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->GetValue();
    if (num_complete % FLAGS_log_mem_usage_interval == 0) {
      char buf[2048];
      // This outputs how much memory is currently being used by this impalad
      MallocExtension::instance()->GetStats(buf, 2048);
      LOG(INFO) << buf;
    }
  }
#endif

  // decrement refcount taken in QueryState::Init();
  qs->ReleaseExecResourceRefcount();
  // decrement refcount taken in StartQuery()
  ReleaseQueryState(qs);
}

void QueryExecMgr::ReleaseQueryState(QueryState* qs) {
  DCHECK(qs != nullptr);
  TUniqueId query_id = qs->query_id();
  int32_t cnt = qs->refcnt_.Add(-1);
  // don't reference anything from 'qs' beyond this point, 'qs' might get
  // gc'd out from under us
  qs = nullptr;
  VLOG_QUERY << "ReleaseQueryState(): query_id=" << PrintId(query_id)
             << " refcnt=" << cnt + 1;
  DCHECK_GE(cnt, 0);
  if (cnt > 0) return;

  QueryState* qs_from_map = nullptr;
  {
    ScopedShardedMapRef<QueryState*> map_ref(query_id,
        &ExecEnv::GetInstance()->query_exec_mgr()->qs_map_);
    DCHECK(map_ref.get() != nullptr);

    auto it = map_ref->find(query_id);
    // someone else might have gc'd the entry
    if (it == map_ref->end()) return;
    qs_from_map = it->second;
    DCHECK(qs_from_map->query_ctx().query_id == query_id);
    int32_t cnt = qs_from_map->refcnt_.Load();
    DCHECK_GE(cnt, 0);
    // someone else might have increased the refcnt in the meantime
    if (cnt > 0) return;
    map_ref->erase(it);
  }
  // TODO: send final status report during gc, but do this from a different thread
  delete qs_from_map;
}
