package raftstore

import (
    "fmt"
    "time"

    "github.com/Connor1996/badger"
    "github.com/pingcap-incubator/tinykv/kv/config"
    "github.com/pingcap-incubator/tinykv/kv/raftstore/message"
    "github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
    "github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
    "github.com/pingcap-incubator/tinykv/kv/raftstore/util"
    "github.com/pingcap-incubator/tinykv/kv/util/engine_util"
    "github.com/pingcap-incubator/tinykv/kv/util/worker"
    "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
    "github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
    "github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
    rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
    "github.com/pingcap-incubator/tinykv/raft"
    "github.com/pingcap/errors"
    "github.com/pingcap/log"
)

// 通知回调：请求因 term 过期而被拒绝
func NotifyStaleReq(term uint64, cb *message.Callback) {
    cb.Done(ErrRespStaleCommand(term))
}

// 通知回调：请求因 region 已被移除而失败
func NotifyReqRegionRemoved(regionId uint64, cb *message.Callback) {
    regionNotFound := &util.ErrRegionNotFound{RegionId: regionId}
    resp := ErrResp(regionNotFound)
    cb.Done(resp)
}

// 主动创建 peer（如 bootstrap/split/merge region），region 必须包含本 store 的 peer 信息
func createPeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
    engines *engine_util.Engines, region *metapb.Region) (*peer, error) {
    metaPeer := util.FindPeer(region, storeID) // 查找本 store 对应的 peer 信息
    if metaPeer == nil {
        return nil, errors.Errorf("find no peer for store %d in region %v", storeID, region)
    }
    log.Info(fmt.Sprintf("region %v create peer with ID %d", region, metaPeer.Id))
    return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

// 通过 raft 成员变更从其他节点创建 peer，仅有 region_id 和 peer_id，region 信息后续通过快照获取
func replicatePeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
    engines *engine_util.Engines, regionID uint64, metaPeer *metapb.Peer) (*peer, error) {
    log.Info(fmt.Sprintf("[region %v] replicates peer with ID %d", regionID, metaPeer.GetId()))
    region := &metapb.Region{
        Id:          regionID,
        RegionEpoch: &metapb.RegionEpoch{},
    }
    return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

// peer 结构体，代表一个 region 的 raft 成员
type peer struct {
    stopped bool // 标记 peer 是否已停止（销毁）

    ticker *ticker // 定时器，驱动 raft tick、日志 GC、心跳、分裂检查等

    Meta     *metapb.Peer // 本 peer 的元信息
    regionId uint64       // region ID

    RaftGroup *raft.RawNode    // raft 实例
    peerStorage *PeerStorage   // raft 存储

    applyProposals []*proposal // 已提交但未应用的 proposal 回调

    peerCache map[uint64]*metapb.Peer // 缓存其他 store 的 peer 信息

    PeersStartPendingTime map[uint64]time.Time // 记录 peer 加入配置的时间

    SizeDiffHint uint64     // region 大小变化估算
    ApproximateSize *uint64 // region 近似大小

    Tag string // 日志标签

    LastApplyingIdx uint64 // 上次应用的日志索引
    LastCompactedIdx uint64 // 上次 GC 的日志索引
}

// 创建新的 peer 实例
func NewPeer(storeId uint64, cfg *config.Config, engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task,
    meta *metapb.Peer) (*peer, error) {
    if meta.GetId() == util.InvalidID {
        return nil, fmt.Errorf("invalid peer id")
    }
    tag := fmt.Sprintf("[region %v] %v", region.GetId(), meta.GetId())

    ps, err := NewPeerStorage(engines, region, regionSched, meta.GetId(), tag) // 创建 raft 存储
    if err != nil {
        return nil, err
    }

    appliedIndex := ps.AppliedIndex() // 获取已应用的日志索引

    raftCfg := &raft.Config{
        ID:            meta.GetId(),
        ElectionTick:  cfg.RaftElectionTimeoutTicks,
        HeartbeatTick: cfg.RaftHeartbeatTicks,
        Applied:       appliedIndex,
        Storage:       ps,
    }

    raftGroup, err := raft.NewRawNode(raftCfg) // 创建 raft 实例
    if err != nil {
        return nil, err
    }
    p := &peer{
        Meta:                  meta,
        regionId:              region.GetId(),
        RaftGroup:             raftGroup,
        peerStorage:           ps,
        peerCache:             make(map[uint64]*metapb.Peer),
        PeersStartPendingTime: make(map[uint64]time.Time),
        Tag:                   tag,
        LastApplyingIdx:       appliedIndex,
        ticker:                newTicker(region.GetId(), cfg),
    }

    // 如果 region 只有一个 peer 且是本 store，直接发起竞选
    if len(region.GetPeers()) == 1 && region.GetPeers()[0].GetStoreId() == storeId {
        err = p.RaftGroup.Campaign()
        if err != nil {
            return nil, err
        }
    }

    return p, nil
}

// 将 peer 信息加入缓存
func (p *peer) insertPeerCache(peer *metapb.Peer) {
    p.peerCache[peer.GetId()] = peer
}

// 从缓存移除 peer
func (p *peer) removePeerCache(peerID uint64) {
    delete(p.peerCache, peerID)
}

// 获取指定 peerID 的 peer 信息（优先缓存，否则从 region 查找并缓存）
func (p *peer) getPeerFromCache(peerID uint64) *metapb.Peer {
    if peer, ok := p.peerCache[peerID]; ok {
        return peer
    }
    for _, peer := range p.peerStorage.Region().GetPeers() {
        if peer.GetId() == peerID {
            p.insertPeerCache(peer)
            return peer
        }
    }
    return nil
}

// 获取下一个 proposal 的日志索引
func (p *peer) nextProposalIndex() uint64 {
    return p.RaftGroup.Raft.RaftLog.LastIndex() + 1
}

// 尝试销毁自身，返回是否需要进一步清理
func (p *peer) MaybeDestroy() bool {
    if p.stopped {
        log.Info(fmt.Sprintf("%v is being destroyed, skip", p.Tag))
        return false
    }
    return true
}

// 真正执行销毁操作，包括设置 tombstone、清理数据、通知所有挂起请求
func (p *peer) Destroy(engine *engine_util.Engines, keepData bool) error {
    start := time.Now()
    region := p.Region()
    log.Info(fmt.Sprintf("%v begin to destroy", p.Tag))

    kvWB := new(engine_util.WriteBatch)   // 创建 kv 写批次
    raftWB := new(engine_util.WriteBatch) // 创建 raft 写批次
    if err := p.peerStorage.clearMeta(kvWB, raftWB); err != nil {
        return err
    }
    meta.WriteRegionState(kvWB, region, rspb.PeerState_Tombstone) // 设置 region 状态为 tombstone
    if err := kvWB.WriteToDB(engine.Kv); err != nil { // 先写 kv，防止重启丢失
        return err
    }
    if err := raftWB.WriteToDB(engine.Raft); err != nil {
        return err
    }

    if p.peerStorage.isInitialized() && !keepData {
        p.peerStorage.ClearData() // 清理 region 数据
    }

    for _, proposal := range p.applyProposals {
        NotifyReqRegionRemoved(region.Id, proposal.cb) // 通知所有挂起请求 region 已被移除
    }
    p.applyProposals = nil

    log.Info(fmt.Sprintf("%v destroy itself, takes %v", p.Tag, time.Now().Sub(start)))
    return nil
}

// 判断 peer 是否已初始化
func (p *peer) isInitialized() bool {
    return p.peerStorage.isInitialized()
}

// 获取本 peer 所属的 store ID
func (p *peer) storeID() uint64 {
    return p.Meta.StoreId
}

// 获取当前 region 信息
func (p *peer) Region() *metapb.Region {
    return p.peerStorage.Region()
}

// 设置 region 信息
func (p *peer) SetRegion(region *metapb.Region) {
    p.peerStorage.SetRegion(region)
}

// 获取本 peer 的 ID
func (p *peer) PeerId() uint64 {
    return p.Meta.GetId()
}

// 获取当前 raft group 的 leader ID
func (p *peer) LeaderId() uint64 {
    return p.RaftGroup.Raft.Lead
}

// 判断本 peer 是否为 leader
func (p *peer) IsLeader() bool {
    return p.RaftGroup.Raft.State == raft.StateLeader
}

// 判断 raft group 是否有尚未提交的快照
func (p *peer) HasPendingSnapshot() bool {
    return p.RaftGroup.GetSnap() != nil
}

// 发送 raft 消息到其他 peer
func (p *peer) Send(trans Transport, msgs []eraftpb.Message) {
    for _, msg := range msgs {
        err := p.sendRaftMessage(msg, trans)
        if err != nil {
            log.Debug(fmt.Sprintf("%v send message err: %v", p.Tag, err))
        }
    }
}

// 收集所有 pending 状态的 peer，并更新 PeersStartPendingTime
func (p *peer) CollectPendingPeers() []*metapb.Peer {
    pendingPeers := make([]*metapb.Peer, 0, len(p.Region().GetPeers()))
    truncatedIdx := p.peerStorage.truncatedIndex()
    for id, progress := range p.RaftGroup.GetProgress() {
        if id == p.Meta.GetId() {
            continue
        }
        if progress.Match < truncatedIdx {
            if peer := p.getPeerFromCache(id); peer != nil {
                pendingPeers = append(pendingPeers, peer)
                if _, ok := p.PeersStartPendingTime[id]; !ok {
                    now := time.Now()
                    p.PeersStartPendingTime[id] = now
                    log.Debug(fmt.Sprintf("%v peer %v start pending at %v", p.Tag, id, now))
                }
            }
        }
    }
    return pendingPeers
}

// 清空所有 pending peer 的时间记录
func (p *peer) clearPeersStartPendingTime() {
    for id := range p.PeersStartPendingTime {
        delete(p.PeersStartPendingTime, id)
    }
}

// 判断是否有新 peer catch up 日志，并更新 PeersStartPendingTime
func (p *peer) AnyNewPeerCatchUp(peerId uint64) bool {
    if len(p.PeersStartPendingTime) == 0 {
        return false
    }
    if !p.IsLeader() {
        p.clearPeersStartPendingTime()
        return false
    }
    if startPendingTime, ok := p.PeersStartPendingTime[peerId]; ok {
        truncatedIdx := p.peerStorage.truncatedIndex()
        progress, ok := p.RaftGroup.Raft.Prs[peerId]
        if ok {
            if progress.Match >= truncatedIdx {
                delete(p.PeersStartPendingTime, peerId)
                elapsed := time.Since(startPendingTime)
                log.Debug(fmt.Sprintf("%v peer %v has caught up logs, elapsed: %v", p.Tag, peerId, elapsed))
                return true
            }
        }
    }
    return false
}

// 判断是否可以处理 pending 快照（apply worker 已完成）
func (p *peer) ReadyToHandlePendingSnap() bool {
    // If apply worker is still working, written apply state may be overwritten
    // by apply worker. So we have to wait here.
    // Please note that committed_index can't be used here. When applying a snapshot,
    // a stale heartbeat can make the leader think follower has already applied
    // the snapshot, and send remaining log entries, which may increase committed_index.
    return p.LastApplyingIdx == p.peerStorage.AppliedIndex()
}

// 获取并清空所有待应用 proposal
func (p *peer) TakeApplyProposals() *MsgApplyProposal {
    if len(p.applyProposals) == 0 {
        return nil
    }
    props := p.applyProposals
    p.applyProposals = nil
    return &MsgApplyProposal{
        Id:       p.PeerId(),
        RegionId: p.regionId,
        Props:    props,
    }
}

// 处理 raft ready 状态（持久化、应用日志、快照等）
func (p *peer) HandleRaftReady(msgs []message.Msg, pdScheduler chan<- worker.Task, trans Transport) (*ApplySnapResult, []message.Msg) {

    // 如果当前 peer 已经被销毁或停止，直接返回，不做任何处理
    if p.stopped {
        return nil, msgs
    }

    // 如果有快照但尚未准备好，则记录日志并返回，等待下一次处理
    if p.HasPendingSnapshot() && !p.ReadyToHandlePendingSnap() {
        log.Debug(fmt.Sprintf("%v [apply_id: %v, last_applying_idx: %v] is not ready to apply snapshot.", p.Tag, p.peerStorage.AppliedIndex(), p.LastApplyingIdx))
        return nil, msgs
    }

    // YOUR CODE HERE (lab1). There are some missing code pars marked with `Hint` above, try to finish them.
    // Hint1: check if there's ready to be processed, if no return directly.
    // 检查 raft group 是否有 ready 状态需要处理，没有则直接返回
    if !p.RaftGroup.HasReady() {
        log.Debug(fmt.Sprintf("%v no raft ready", p.Tag))
        return nil, msgs
    }

    // 开始处理 ready 状态
    log.Debug(fmt.Sprintf("%v handle raft ready", p.Tag))

    // 获取 raft group 的 ready 对象，包含需要处理的日志、快照、消息等
    ready := p.RaftGroup.Ready()
    // TODO: workaround for:
    //   in kvproto/eraftpb, we use *SnapshotMetadata
    //   but in etcd, they use SnapshotMetadata
    // 如果 Ready 中包含快照，但元数据为空，则初始化元数据（兼容性处理）
    if ready.Snapshot.GetMetadata() == nil {
        ready.Snapshot.Metadata = &eraftpb.SnapshotMetadata{}
    }

    // 如果当前节点是 leader，先发送消息
    if p.IsLeader() {
        p.Send(trans, ready.Messages)       // 发送 raft 消息
        ready.Messages = ready.Messages[:0] // 清空消息，避免重复发送
    }

    // 处理软状态（如果 ready 中包含）
    ss := ready.SoftState
    if ss != nil && ss.RaftState == raft.StateLeader {
        p.HeartbeatScheduler(pdScheduler) // 发送心跳任务，通知调度器
    }

    // 持久化日志条目和快照，防止数据丢失，更新硬状态
    applySnapResult, err := p.peerStorage.SaveReadyState(&ready)
    if err != nil {
        panic(fmt.Sprintf("failed to handle raft ready, error: %v", err))
    }
    if !p.IsLeader() { // 如果当前节点不是 leader，再次发送消息，确保日志或快照同步
        p.Send(trans, ready.Messages)
    }

    // 处理快照和日志条目，确保状态机的正确更新，优先处理快照
    if applySnapResult != nil {
        // 如果快照应用成功，生成刷新消息并加入消息队列，并更新快照的应用索引
        msgs = append(msgs, message.NewPeerMsg(message.MsgTypeApplyRefresh, p.regionId, &MsgApplyRefresh{
            id:     p.PeerId(),
            term:   p.Term(),
            region: p.Region(),
        }))
        p.LastApplyingIdx = p.peerStorage.truncatedIndex()
    } else {
        // 没有新的快照应用，则处理 Ready 中的已提交日志条目
        committedEntries := ready.CommittedEntries
        ready.CommittedEntries = nil
        l := len(committedEntries)
        if l > 0 {
            p.LastApplyingIdx = committedEntries[l-1].Index
            msgs = append(msgs, message.Msg{Type: message.MsgTypeApplyCommitted, Data: &MsgApplyCommitted{
                regionId: p.regionId,
                term:     p.Term(),
                entries:  committedEntries,
            }, RegionID: p.regionId})
        }
    }

    // YOUR CODE HERE (lab1). There are some missing code pars marked with `Hint` above, try to finish them.
    // Hint2: Try to advance the states in the raft group of this peer after processing the raft ready.
    //        Check about the `Advance` method in for the raft group.
    // 推进 Raft 的状态，使其知道之前的 Ready 状态已经被处理，进入下一步
    p.RaftGroup.Advance(ready)

    return applySnapResult, msgs
}

// 判断是否需要竞选为 leader（如分裂后）
func (p *peer) MaybeCampaign(parentIsLeader bool) bool {
    if len(p.Region().GetPeers()) <= 1 || !parentIsLeader {
        return false
    }
    p.RaftGroup.Campaign()
    return true
}

// 获取当前任期
func (p *peer) Term() uint64 {
    return p.RaftGroup.Raft.Term
}

// 向调度器发送 region 心跳
func (p *peer) HeartbeatScheduler(ch chan<- worker.Task) {
    clonedRegion := new(metapb.Region)
    err := util.CloneMsg(p.Region(), clonedRegion)
    if err != nil {
        return
    }
    ch <- &runner.SchedulerRegionHeartbeatTask{
        Region:          clonedRegion,
        Peer:            p.Meta,
        PendingPeers:    p.CollectPendingPeers(),
        ApproximateSize: p.ApproximateSize,
    }
}

// 发送 raft 消息到目标 peer
func (p *peer) sendRaftMessage(msg eraftpb.Message, trans Transport) error {
    sendMsg := new(rspb.RaftMessage)
    sendMsg.RegionId = p.regionId
    // 设置当前 epoch
    sendMsg.RegionEpoch = &metapb.RegionEpoch{
        ConfVer: p.Region().RegionEpoch.ConfVer,
        Version: p.Region().RegionEpoch.Version,
    }

    fromPeer := *p.Meta
    toPeer := p.getPeerFromCache(msg.To)
    if toPeer == nil {
        return fmt.Errorf("failed to lookup recipient peer %v in region %v", msg.To, p.regionId)
    }
    log.Debug(fmt.Sprintf("%v, send raft msg %v from %v to %v", p.Tag, msg.MsgType, fromPeer, toPeer))

    sendMsg.FromPeer = &fromPeer
    sendMsg.ToPeer = toPeer

    // 对于新建/投票/心跳消息，附带 region 的起止 key
    if p.peerStorage.isInitialized() && util.IsInitialMsg(&msg) {
        sendMsg.StartKey = append([]byte{}, p.Region().StartKey...)
        sendMsg.EndKey = append([]byte{}, p.Region().EndKey...)
    }
    sendMsg.Message = &msg
    return trans.Send(sendMsg)
}

// 提议一个请求，返回 true 表示成功
func (p *peer) Propose(kv *badger.DB, cfg *config.Config, cb *message.Callback, req *raft_cmdpb.RaftCmdRequest, errResp *raft_cmdpb.RaftCmdResponse) bool {
    if p.stopped {
        return false
    }

    isConfChange := false

    policy, err := p.inspect(req)
    if err != nil {
        BindRespError(errResp, err)
        cb.Done(errResp)
        return false
    }
    var idx uint64
    switch policy {
    case RequestPolicy_ProposeNormal:
        idx, err = p.ProposeNormal(cfg, req)
    case RequestPolicy_ProposeTransferLeader:
        return p.ProposeTransferLeader(cfg, req, cb)
    case RequestPolicy_ProposeConfChange:
        isConfChange = true
        idx, err = p.ProposeConfChange(cfg, req)
    }

    if err != nil {
        BindRespError(errResp, err)
        cb.Done(errResp)
        return false
    }

    p.PostPropose(idx, p.Term(), isConfChange, cb)
    return true
}

// 记录 proposal 回调
func (p *peer) PostPropose(index, term uint64, isConfChange bool, cb *message.Callback) {
    proposal := &proposal{
        isConfChange: isConfChange,
        index:        index,
        term:         term,
        cb:           cb,
    }
    p.applyProposals = append(p.applyProposals, proposal)
}

// 统计健康节点数（日志已追上）
func (p *peer) countHealthyNode(progress map[uint64]raft.Progress) int {
    healthy := 0
    for _, pr := range progress {
        if pr.Match >= p.peerStorage.truncatedIndex() {
            healthy += 1
        }
    }
    return healthy
}

// 校验配置变更请求是否安全（变更后仍有法定节点存活）
func (p *peer) checkConfChange(cfg *config.Config, cmd *raft_cmdpb.RaftCmdRequest) error {
    changePeer := GetChangePeerCmd(cmd)
    changeType := changePeer.GetChangeType()
    peer := changePeer.GetPeer()

    progress := p.RaftGroup.GetProgress()
    total := len(progress)
    if total <= 1 {
        return nil
    }

    switch changeType {
    case eraftpb.ConfChangeType_AddNode:
        progress[peer.Id] = raft.Progress{}
    case eraftpb.ConfChangeType_RemoveNode:
        if _, ok := progress[peer.Id]; ok {
            delete(progress, peer.Id)
        } else {
            return nil
        }
    }

    healthy := p.countHealthyNode(progress)
    quorumAfterChange := Quorum(len(progress))
    if healthy >= quorumAfterChange {
        return nil
    }

    log.Info(fmt.Sprintf("%v rejects unsafe conf chagne request %v, total %v, healthy %v quorum after change %v",
        p.Tag, changePeer, total, healthy, quorumAfterChange))

    return fmt.Errorf("unsafe to perform conf change %v, total %v, healthy %v, quorum after chagne %v",
        changePeer, total, healthy, quorumAfterChange)
}

// 计算法定节点数
func Quorum(total int) int {
    return total/2 + 1
}

// 发起 leader 转移
func (p *peer) transferLeader(peer *metapb.Peer) {
    log.Info(fmt.Sprintf("%v transfer leader to %v", p.Tag, peer))
    p.RaftGroup.TransferLeader(peer.GetId())
}

// 普通请求提议
func (p *peer) ProposeNormal(cfg *config.Config, req *raft_cmdpb.RaftCmdRequest) (uint64, error) {
    data, err := req.Marshal()
    if err != nil {
        return 0, err
    }

    proposeIndex := p.nextProposalIndex()
    err = p.RaftGroup.Propose(data)
    if err != nil {
        return 0, err
    }
    if proposeIndex == p.nextProposalIndex() {
        // 消息被静默丢弃，通常是 leader 不存在或正在转移
        return 0, &util.ErrNotLeader{RegionId: p.regionId}
    }

    return proposeIndex, nil
}

// leader 转移请求，返回 true 表示已接受
func (p *peer) ProposeTransferLeader(cfg *config.Config, req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) bool {
    transferLeader := getTransferLeaderCmd(req)
    peer := transferLeader.Peer

    p.transferLeader(peer)
    // 转移 leader 命令无需复制日志，直接返回
    cb.Done(makeTransferLeaderResponse())

    return true
}

// 配置变更请求提议
func (p *peer) ProposeConfChange(cfg *config.Config, req *raft_cmdpb.RaftCmdRequest) (uint64, error) {
    if p.RaftGroup.Raft.PendingConfIndex > p.peerStorage.AppliedIndex() {
        log.Info(fmt.Sprintf("%v there is a pending conf change, try later", p.Tag))
        return 0, fmt.Errorf("%v there is a pending conf change, try later", p.Tag)
    }

    if err := p.checkConfChange(cfg, req); err != nil {
        return 0, err
    }

    data, err := req.Marshal()
    if err != nil {
        return 0, err
    }

    changePeer := GetChangePeerCmd(req)
    var cc eraftpb.ConfChange
    cc.ChangeType = changePeer.ChangeType
    cc.NodeId = changePeer.Peer.Id
    cc.Context = data

    log.Info(fmt.Sprintf("%v propose conf change %v peer %v", p.Tag, cc.ChangeType, cc.NodeId))

    proposeIndex := p.nextProposalIndex()
    if err = p.RaftGroup.ProposeConfChange(cc); err != nil {
        return 0, err
    }
    if p.nextProposalIndex() == proposeIndex {
        return 0, &util.ErrNotLeader{RegionId: p.regionId}
    }

    return proposeIndex, nil
}

// 请求策略类型
type RequestPolicy int

const (
    RequestPolicy_ProposeNormal RequestPolicy = 0 + iota // 普通请求
    RequestPolicy_ProposeTransferLeader                  // leader 转移请求
    RequestPolicy_ProposeConfChange                      // 配置变更请求
    RequestPolicy_Invalid                                // 非法请求
)

// 检查请求类型，返回对应策略
func (p *peer) inspect(req *raft_cmdpb.RaftCmdRequest) (RequestPolicy, error) {
    if req.AdminRequest != nil {
        if GetChangePeerCmd(req) != nil {
            return RequestPolicy_ProposeConfChange, nil
        }
        if getTransferLeaderCmd(req) != nil {
            return RequestPolicy_ProposeTransferLeader, nil
        }
    }

    hasRead, hasWrite := false, false
    for _, r := range req.Requests {
        switch r.CmdType {
        case raft_cmdpb.CmdType_Get, raft_cmdpb.CmdType_Snap:
            hasRead = true
        case raft_cmdpb.CmdType_Delete, raft_cmdpb.CmdType_Put:
            hasWrite = true
        case raft_cmdpb.CmdType_Invalid:
            return RequestPolicy_Invalid, fmt.Errorf("invalid cmd type %v, message maybe corrupted", r.CmdType)
        }

        if hasRead && hasWrite {
            return RequestPolicy_Invalid, fmt.Errorf("read and write can't be mixed in one request.")
        }
    }
    return RequestPolicy_ProposeNormal, nil
}

// 获取 transfer leader 命令
func getTransferLeaderCmd(req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.TransferLeaderRequest {
    if req.AdminRequest == nil {
        return nil
    }
    return req.AdminRequest.TransferLeader
}

// 构造 transfer leader 响应
func makeTransferLeaderResponse() *raft_cmdpb.RaftCmdResponse {
    adminResp := &raft_cmdpb.AdminResponse{}
    adminResp.CmdType = raft_cmdpb.AdminCmdType_TransferLeader
    adminResp.TransferLeader = &raft_cmdpb.TransferLeaderResponse{}
    resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
    resp.AdminResponse = adminResp
    return resp
}

// 获取 change peer 命令
func GetChangePeerCmd(msg *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.ChangePeerRequest {
    if msg.AdminRequest == nil || msg.AdminRequest.ChangePeer == nil {
        return nil
    }
    return msg.AdminRequest.ChangePeer
}