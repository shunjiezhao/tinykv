// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	// bigRegion
	var suitableStore []*core.StoreInfo
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			suitableStore = append(suitableStore, store)
		}
	}
	if len(suitableStore) == 0 {
		log.Info("don't have any store")
		return nil
	}
	if len(suitableStore) == 1 {
		log.Info("only have one store")
		return nil
	}

	sort.Slice(suitableStore, func(i, j int) bool {
		return suitableStore[i].GetRegionSize() > suitableStore[j].GetRegionSize()
	})
	var bigRegion *core.RegionInfo
	var bigStore, smallStore *core.StoreInfo

	for _, store := range suitableStore {
		type hander func(uint64, func(core.RegionsContainer))
		for _, h := range []hander{cluster.GetPendingRegionsWithLock, cluster.GetFollowersWithLock, cluster.GetLeadersWithLock} {
			h(store.GetID(), func(container core.RegionsContainer) {
				region := container.RandomRegion(nil, nil)

				if region != nil {
					bigRegion = region
					bigStore = store
				}
			})
			if bigRegion != nil {
				goto done
			}
		}
	}

	return nil
done:
	ids := bigRegion.GetStoreIds()
	if len(ids) < cluster.GetMaxReplicas() {
		log.Info("region don't have enough replicas")
		return nil
	}
	for i := len(suitableStore) - 1; i >= 0; i-- {
		if _, ok := ids[suitableStore[i].GetID()]; !ok {
			if bigStore.GetRegionSize()-suitableStore[i].GetRegionSize() > 2*bigRegion.GetApproximateSize() {
				smallStore = suitableStore[i]
				break
			}
		}
	}
	if smallStore == nil {
		log.Info("don't have suitable store")
		return nil
	}

	peer, err := cluster.AllocPeer(smallStore.GetID())

	if err != nil {
		panic(err)
	}
	op, err := operator.CreateMovePeerOperator("balance-region", cluster, bigRegion, operator.OpBalance, bigStore.GetID(), smallStore.GetID(), peer.GetId())
	if err != nil {
		panic(err)
	}

	return op

}
