package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	peerID uint64
	// current region information of the peer
	region *metapb.Region
	// current raft state of the peer
	raftState rspb.RaftLocalState
	// current snapshot state
	snapState snap.SnapState
	// regionSched used to schedule task to region worker
	regionSched chan<- worker.Task
	// gennerate snapshot tried count
	snapTriedCnt int
	// Engine include two badger instance: Raft and Kv
	Engines *engine_util.Engines
	// Tag used for logging
	Tag string
}

// NewPeerStorage get the persist raftState from engines and return a peer storage
func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, peerID uint64, tag string) (*PeerStorage, error) {
	log.Debug(fmt.Sprintf("%s creating storage for %s", tag, region.String()))
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	if err != nil {
		return nil, err
	}
	applyState, err := meta.InitApplyState(engines.Kv, region)
	if err != nil {
		return nil, err
	}
	if raftState.LastIndex < applyState.AppliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	return &PeerStorage{
		Engines:     engines,
		peerID:      peerID,
		region:      region,
		Tag:         tag,
		raftState:   *raftState,
		regionSched: regionSched,
	}, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	if err := ps.checkRange(low, high); err != nil || low == high {
		return nil, err
	}
	buf := make([]eraftpb.Entry, 0, high-low)
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(ps.region.Id, low)
	endKey := meta.RaftLogKey(ps.region.Id, high)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	return nil, raft.ErrUnavailable
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		return ps.raftState.LastTerm, nil
	}
	var entry eraftpb.Entry
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snapshot eraftpb.Snapshot
	if ps.snapState.StateType == snap.SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			if s != nil {
				snapshot = *s
			}
		default:
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warn(fmt.Sprintf("failed to try generating snapshot, regionID: %d, peerID: %d, times: %d", ps.region.GetId(), ps.peerID, ps.snapTriedCnt))
		}
	}

	if ps.snapTriedCnt >= 5 {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	log.Info(fmt.Sprintf("requesting snapshot, regionID: %d, peerID: %d", ps.region.GetId(), ps.peerID))
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	// schedule snapshot generate task
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState().TruncatedState.Index
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState().TruncatedState.Term
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState().AppliedIndex
}

func (ps *PeerStorage) applyState() *rspb.RaftApplyState {
	state, _ := meta.GetApplyState(ps.Engines.Kv, ps.region.GetId())
	return state
}

func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Info(fmt.Sprintf("snapshot is stale, generate again, regionID: %d, peerID: %d, snapIndex: %d, truncatedIndex: %d", ps.region.GetId(), ps.peerID, idx, ps.truncatedIndex()))
		return false
	}
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil {
		log.Error(fmt.Sprintf("failed to decode snapshot, it may be corrupted, regionID: %d, peerID: %d, err: %v", ps.region.GetId(), ps.peerID, err))
		return false
	}
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Info(fmt.Sprintf("snapshot epoch is stale, regionID: %d, peerID: %d, snapEpoch: %s, latestEpoch: %s", ps.region.GetId(), ps.peerID, snapEpoch, latestEpoch))
		return false
	}
	return true
}

// Append 将指定的日志条目追加到 Raft 日志中
// 这是 Raft 日志管理的核心方法，负责：
// 1. 持久化新的日志条目到 Raft 存储引擎
// 2. 清理与新日志产生冲突的历史条目
// 3. 更新本地 Raft 状态的索引和任期信息
//
// 在 Raft 协议中，当节点接收到新的日志条目时，必须确保日志的一致性。
// 如果新日志与现有日志存在冲突（相同索引但不同内容），则需要删除冲突的条目。
//
// 参数说明：
//   entries: 需要追加的日志条目数组
//   raftWB: Raft 引擎的批量写入对象，用于批量操作
// 返回值：
//   error: 处理过程中遇到的错误
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
    log.Debug(fmt.Sprintf("%s append %d entries", ps.Tag, len(entries)))

    // 记录当前的最后日志索引，用于后续的冲突检测
    originalLastIndex := ps.raftState.LastIndex 

    // === 步骤一：输入合法性检查 ===
    // 如果传入的日志条目列表为空，无需进行任何处理
    if len(entries) == 0 {
        return nil
    }

    // === 步骤二：获取新日志的关键信息 ===
    // 获取要追加的日志条目中的最后一个条目
    finalLogEntry := entries[len(entries)-1] 
    lastIndex := finalLogEntry.Index
    lastTerm := finalLogEntry.Term

    // YOUR CODE HERE (lab1).
    // === 步骤三：持久化所有新的日志条目 ===
    // 遍历每个日志条目，将其写入到 Raft 存储引擎中
    for _, entry := range entries {
        // Hint1: in the raft write batch, the log key could be generated by `meta.RaftLogKey`.
        //       Also the `LastIndex` and `LastTerm` raft states should be updated after the `Append`.
        //       Use the input `raftWB` to save the append results, do check if the input `entries` are empty.
        //       Note the raft logs are stored as the `meta` type key-value pairs, so the `RaftLogKey` and `SetMeta`
        //       functions could be useful.
        // 提示1：在 Raft 写批次中，日志键可以通过 `meta.RaftLogKey` 生成。
        //       同时，`LastIndex` 和 `LastTerm` Raft 状态应该在追加日志条目后更新。
        //       使用输入的 `raftWB` 保存追加结果，检查输入的 `entries` 是否为空。
        //       注意，Raft 日志存储为 `meta` 类型的键值对，因此 `RaftLogKey` 和 `SetMeta` 函数可能会有用。

        // 为每个日志条目生成在存储中的唯一标识键
        // 键的格式包含 region ID 和日志索引，确保全局唯一性
        key := meta.RaftLogKey(ps.region.GetId(), entry.GetIndex())
        
        // 将日志条目序列化并添加到批量写入操作中
        // SetMeta 方法会处理序列化和存储的细节
        raftWB.SetMeta(key, &entry)
        
        log.Debug(fmt.Sprintf("Prepared to persist log entry: index=%d, term=%d", 
            entry.GetIndex(), entry.GetTerm()))
    }

    // === 步骤四：清理冲突的历史日志条目 ===
    // 根据 Raft 协议，当接收到新的日志条目时，需要删除所有索引大于新日志的旧条目
    // 这确保了日志的一致性：新的条目会覆盖可能存在冲突的旧条目
    for i := lastIndex + 1; i <= originalLastIndex; i++ {
        // Hint2: As the to be append logs may conflict with the old ones, try to delete the left
        //       old ones whose entry indexes are greater than the last to be append entry.
        //       Delete these previously appended log entries which will never be committed.
        // 提示2：由于要追加的日志条目可能与旧的日志条目冲突，尝试删除索引大于最后一个要追加的日志条目的旧日志条目。
        //       删除这些之前追加的但永远不会被提交的日志条目。
        
        // 生成要删除的冲突日志条目的存储键
        key := meta.RaftLogKey(ps.region.GetId(), i)
        
        // 将删除操作添加到批量写入中
        raftWB.DeleteMeta(key)
        
        log.Debug(fmt.Sprintf("Marked conflicting log entry for deletion: index=%d", i))
    }

    // === 步骤五：更新本地 Raft 状态 ===
    // 更新当前节点维护的 Raft 状态信息
    // 这些状态会在后续的 SaveReadyState 中持久化到存储
    ps.raftState.LastIndex = lastIndex  // 更新最后日志索引
    ps.raftState.LastTerm = lastTerm    // 更新最后日志任期
    
    log.Debug(fmt.Sprintf("Updated local raft state: LastIndex=%d, LastTerm=%d", 
        lastIndex, lastTerm))

    return nil
}

func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// Delete all data that is not covered by `new_region`.
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.clearRange(newRegion.Id, oldStartKey, newStartKey)
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 {
		ps.clearRange(newRegion.Id, newEndKey, oldEndKey)
	}
}

func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.DeleteMeta(meta.RegionStateKey(regionID))
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))

	firstIndex := lastIndex + 1
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	log.Info(fmt.Sprintf(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	))
	return nil
}

// Apply the peer with given snapshot.
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Info(fmt.Sprintf("%v begin to apply snapshot", ps.Tag))

	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}

	if snapData.Region.Id != ps.region.Id {
		return nil, fmt.Errorf("mismatch region id %v != %v", snapData.Region.Id, ps.region.Id)
	}

	if ps.isInitialized() {
		// we can only delete the old data when the peer is initialized.
		if err := ps.clearMeta(kvWB, raftWB); err != nil {
			return nil, err
		}
		ps.clearExtraData(snapData.Region)
	}

	ps.raftState.LastIndex = snapshot.Metadata.Index
	ps.raftState.LastTerm = snapshot.Metadata.Term

	applyRes := &ApplySnapResult{
		PrevRegion: ps.region,
		Region:     snapData.Region,
	}
	ps.region = snapData.Region
	applyState := &rspb.RaftApplyState{
		AppliedIndex: snapshot.Metadata.Index,
		// The snapshot only contains log which index > applied index, so
		// here the truncate state's (index, term) is in snapshot metadata.
		TruncatedState: &rspb.RaftTruncatedState{
			Index: snapshot.Metadata.Index,
			Term:  snapshot.Metadata.Term,
		},
	}
	kvWB.SetMeta(meta.ApplyStateKey(ps.region.GetId()), applyState)
	meta.WriteRegionState(kvWB, snapData.Region, rspb.PeerState_Normal)
	ch := make(chan bool)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Applying,
	}
	ps.regionSched <- &runner.RegionTaskApply{
		RegionId: ps.region.Id,
		Notifier: ch,
		SnapMeta: snapshot.Metadata,
		StartKey: snapData.Region.GetStartKey(),
		EndKey:   snapData.Region.GetEndKey(),
	}
	// wait until apply finish
	<-ch

	log.Debug(fmt.Sprintf("%v apply snapshot for region %v with state %v ok", ps.Tag, snapData.Region, applyState))
	return applyRes, nil
}

// SaveReadyState 处理由 Raft 实例生成的 Ready 状态
// 这是 Raft 持久化的核心方法，主要任务包括：
// 1. 处理快照应用（如果有）
// 2. 持久化新的日志条目
// 3. 更新并持久化硬状态（HardState）
// 4. 将所有更改提交到存储引擎
//
// 重要说明：不要在此函数中修改 ready 对象，这是后续正确推进 ready 对象的要求
//
// 参数：
//   ready: 包含需要持久化的状态变更的 Ready 对象
// 返回：
//   *ApplySnapResult: 快照应用结果（如果应用了快照）
//   error: 处理过程中的错误
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
    // === 第一步：初始化批量写入对象 ===
    // kvWB 用于状态机相关的写入（快照数据、应用状态等）
    // raftWB 用于 Raft 日志相关的写入（日志条目、Raft 状态等）
    kvWB, raftWB := new(engine_util.WriteBatch), new(engine_util.WriteBatch)

    // 保存当前 Raft 状态的副本，用于后续比较是否发生变化
    // 只有状态确实改变时才需要持久化，避免不必要的写入
    currentRaftState := ps.raftState 
    var snapshotResult *ApplySnapResult = nil 
    var processingError error

    // === 第二步：处理快照应用（如果存在）===
    // 快照应用的优先级最高，因为它会重置整个状态机
    if !raft.IsEmptySnap(&ready.Snapshot) {
        log.Debug(fmt.Sprintf("%s applying snapshot with index=%d", 
            ps.Tag, ready.Snapshot.Metadata.Index))
        
        snapshotResult, processingError = ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
        if processingError != nil {
            return nil, fmt.Errorf("failed to apply snapshot: %w", processingError)
        }
    }

    // YOUR CODE HERE (lab1).
    // Hint: the outputs of the raft ready are: snapshot, entries, states, try to process
    //       them correctly. Note the snapshot apply may need the kv engine while others will
    //       always use the raft engine.
    // 提示：Raft ready 的输出包括：快照、日志条目、状态，尝试正确处理它们。
    //       注意，应用快照可能需要使用 KV 引擎，而其他操作将始终使用 Raft 引擎。

    // === 第三步：处理日志条目持久化 ===
    if len(ready.Entries) != 0 {
        // Hint1: Process entries if it's not empty.
        // 如果日志条目不为空，处理日志条目。
        
        log.Debug(fmt.Sprintf("%s persisting %d new log entries", 
            ps.Tag, len(ready.Entries)))
        
        // 调用 Append 方法将新的日志条目持久化到 Raft 日志存储
        // 这包括：将条目写入存储 + 清理冲突的旧条目 + 更新本地状态
        appendError := ps.Append(ready.Entries, raftWB)
        if appendError != nil {
            return nil, fmt.Errorf("failed to append log entries: %w", appendError)
        }
    }

    // === 第四步：处理硬状态更新 ===
    // LastIndex 为 0 意味着该 peer 是从 Raft 消息创建的
    // 并且尚未应用快照，因此跳过硬状态的持久化
    if ps.raftState.LastIndex > 0 {
        // Hint2: Handle the hard state if it is NOT empty.
        // 提示2：如果硬状态不为空，处理硬状态。
        
        // 检查 Ready 中是否包含需要持久化的硬状态
        // 硬状态包括：当前任期(Term)、投票对象(Vote)、提交索引(Commit)
        if !raft.IsEmptyHardState(ready.HardState) {
            log.Debug(fmt.Sprintf("%s updating hard state: term=%d, vote=%d, commit=%d", 
                ps.Tag, ready.HardState.Term, ready.HardState.Vote, ready.HardState.Commit))
            
            // 更新本地的硬状态
            ps.raftState.HardState = &ready.HardState
        }
    }

    // === 第五步：条件性持久化 Raft 状态 ===
    // 只有当 Raft 状态确实发生变化时才进行持久化
    // 这是一个重要的性能优化：避免不必要的磁盘写入
    if !proto.Equal(&currentRaftState, &ps.raftState) {
        log.Debug(fmt.Sprintf("%s persisting updated raft state", ps.Tag))
        
        // 将更新后的 Raft 状态添加到写批次中
        // RaftStateKey 生成该 region 对应的 Raft 状态存储键
        raftWB.SetMeta(meta.RaftStateKey(ps.region.GetId()), &ps.raftState)
    }

    // === 第六步：原子性提交所有更改 ===
    // 使用 MustWriteToDB 确保所有更改都成功写入存储
    // 如果写入失败，程序会 panic，确保数据一致性
    
    // 提交状态机相关的更改（快照数据、应用状态等）
    kvWB.MustWriteToDB(ps.Engines.Kv)
    
    // 提交 Raft 日志相关的更改（日志条目、Raft 状态等）
    raftWB.MustWriteToDB(ps.Engines.Raft)

    log.Debug(fmt.Sprintf("%s successfully saved ready state", ps.Tag))
    
    return snapshotResult, nil
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) ClearData() {
	ps.clearRange(ps.region.GetId(), ps.region.GetStartKey(), ps.region.GetEndKey())
}

func (ps *PeerStorage) clearRange(regionID uint64, start, end []byte) {
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: regionID,
		StartKey: start,
		EndKey:   end,
	}
}
