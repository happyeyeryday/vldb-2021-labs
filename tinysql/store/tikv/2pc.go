// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"bytes"
	"context"
	"math"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type twoPhaseCommitAction interface {
	handleSingleBatch(*twoPhaseCommitter, *Backoffer, batchKeys) error
	String() string
}

type actionPrewrite struct{}
type actionCommit struct{}
type actionCleanup struct{}

var (
	_ twoPhaseCommitAction = actionPrewrite{}
	_ twoPhaseCommitAction = actionCommit{}
	_ twoPhaseCommitAction = actionCleanup{}
)

// Global variable set by config file.
var (
	ManagedLockTTL uint64 = 20000 // 20s
)

func (actionPrewrite) String() string {
	return "prewrite"
}

func (actionCommit) String() string {
	return "commit"
}

func (actionCleanup) String() string {
	return "cleanup"
}

// twoPhaseCommitter executes a two-phase commit protocol.
type twoPhaseCommitter struct {
	store     *TinykvStore
	txn       *tikvTxn
	startTS   uint64
	keys      [][]byte
	mutations map[string]*mutationEx
	lockTTL   uint64
	commitTS  uint64
	connID    uint64 // connID is used for log.
	cleanWg   sync.WaitGroup
	txnSize   int

	primaryKey []byte

	mu struct {
		sync.RWMutex
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
		committed       bool
	}
	// regionTxnSize stores the number of keys involved in each region
	regionTxnSize map[uint64]int
}

// batchExecutor is txn controller providing rate control like utils
type batchExecutor struct {
	rateLim           int                  // concurrent worker numbers
	rateLimiter       *rateLimit           // rate limiter for concurrency control, maybe more strategies
	committer         *twoPhaseCommitter   // here maybe more different type committer in the future
	action            twoPhaseCommitAction // the work action type
	backoffer         *Backoffer           // Backoffer
	tokenWaitDuration time.Duration        // get token wait time
}

type mutationEx struct {
	pb.Mutation
}

// newTwoPhaseCommitter creates a twoPhaseCommitter.
func newTwoPhaseCommitter(txn *tikvTxn, connID uint64) (*twoPhaseCommitter, error) {
	return &twoPhaseCommitter{
		store:         txn.store,
		txn:           txn,
		startTS:       txn.StartTS(),
		connID:        connID,
		regionTxnSize: map[uint64]int{},
	}, nil
}

// The txn mutations buffered in `txn.us` before commit.
// Your task is to convert buffer to KV mutations in order to execute as a transaction.
// This function runs before commit execution
func (c *twoPhaseCommitter) initKeysAndMutations() error {
	var (
		keys    [][]byte
		size    int
		putCnt  int
		delCnt  int
		lockCnt int
	)
	mutations := make(map[string]*mutationEx)
	txn := c.txn
	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		// In membuffer, there are 2 kinds of mutations
		//   put: there is a new value in membuffer
		//   delete: there is a nil value in membuffer
		// You need to build the mutations from membuffer here
		if len(v) > 0 {
			// `len(v) > 0` means it's a put operation.
			// YOUR CODE HERE (lab3).
			//panic("YOUR CODE HERE")
			mutations[string(k)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:    pb.Op_Put,//put operation
					Key:   k,
					Value: v,
				},
			}
			putCnt++
		} else {
			// `len(v) == 0` means it's a delete operation.
			// YOUR CODE HERE (lab3).
			//panic("YOUR CODE HERE")
			mutations[string(k)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:  pb.Op_Del,//delete operation
					Key: k,
				},
			}
			delCnt++
		}
		// Update the keys array and statistic information
		// YOUR CODE HERE (lab3).
		//panic("YOUR CODE HERE")
		keys = append(keys, k)
		size += len(k) + len(v)
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	// In prewrite phase, there will be a lock for every key
	// If the key is already locked but is not updated, you need to write a lock mutation for it to prevent lost update
	// Don't forget to update the keys array and statistic information
	for _, lockKey := range txn.lockKeys {
		// YOUR CODE HERE (lab3).
		_, ok := mutations[string(lockKey)]
		if !ok {
			//panic("YOUR CODE HERE")
			mutations[string(lockKey)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:  pb.Op_Lock,
					Key: lockKey,
				},
			}
			lockCnt++
			keys = append(keys, lockKey)
			size += len(lockKey)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	c.txnSize = size

	if size > int(kv.TxnTotalSizeLimit) {
		return kv.ErrTxnTooLarge.GenWithStackByArgs(size)
	}
	const logEntryCount = 10000
	const logSize = 4 * 1024 * 1024 // 4MB
	if len(keys) > logEntryCount || size > logSize {
		tableID := tablecodec.DecodeTableID(keys[0])
		logutil.BgLogger().Info("[BIG_TXN]",
			zap.Uint64("con", c.connID),
			zap.Int64("table ID", tableID),
			zap.Int("size", size),
			zap.Int("keys", len(keys)),
			zap.Int("puts", putCnt),
			zap.Int("dels", delCnt),
			zap.Int("locks", lockCnt),
			zap.Uint64("txnStartTS", txn.startTS))
	}

	// Sanity check for startTS.
	if txn.StartTS() == math.MaxUint64 {
		err = errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.BgLogger().Error("commit failed",
			zap.Uint64("conn", c.connID),
			zap.Error(err))
		return errors.Trace(err)
	}

	c.keys = keys
	c.mutations = mutations
	c.lockTTL = txnLockTTL(txn.startTime, size)
	return nil
}

func (c *twoPhaseCommitter) primary() []byte {
	if len(c.primaryKey) == 0 {
		return c.keys[0]
	}
	return c.primaryKey
}

const bytesPerMiB = 1024 * 1024

func txnLockTTL(startTime time.Time, txnSize int) uint64 {
	// Increase lockTTL for large transactions.
	// The formula is `ttl = ttlFactor * sqrt(sizeInMiB)`.
	// When writeSize is less than 256KB, the base ttl is defaultTTL (3s);
	// When writeSize is 1MiB, 100MiB, or 400MiB, ttl is 6s, 60s, 120s correspondingly;
	lockTTL := defaultLockTTL
	if txnSize >= txnCommitBatchSize {
		sizeMiB := float64(txnSize) / bytesPerMiB
		lockTTL = uint64(float64(ttlFactor) * math.Sqrt(sizeMiB))
		if lockTTL < defaultLockTTL {
			lockTTL = defaultLockTTL
		}
		if lockTTL > maxLockTTL {
			lockTTL = maxLockTTL
		}
	}

	// Increase lockTTL by the transaction's read time.
	// When resolving a lock, we compare current ts and startTS+lockTTL to decide whether to clean up. If a txn
	// takes a long time to read, increasing its TTL will help to prevent it from been aborted soon after prewrite.
	elapsed := time.Since(startTime) / time.Millisecond
	return lockTTL + uint64(elapsed)
}

// doActionOnKeys groups keys into primary batch and secondary batches, if primary batch exists in the key,
// it does action on primary batch first, then on secondary batches. If action is commit, secondary batches
// is done in background goroutine.
// There are three kind of actions which implement the twoPhaseCommitAction interface.
// actionPrewrite prewrites a transaction
// actionCommit commits a transaction
// actionCleanup rollbacks a transaction
// This function split the keys by region and parallel execute the batches in a transaction using given action
func (c *twoPhaseCommitter) doActionOnKeys(bo *Backoffer, action twoPhaseCommitAction, keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	groups, firstRegion, err := c.store.regionCache.GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return errors.Trace(err)
	}

	var batches []batchKeys
	var sizeFunc = c.keySize
	if _, ok := action.(actionPrewrite); ok {
		// Do not update regionTxnSize on retries. They are not used when building a PrewriteRequest.
		if len(bo.errors) == 0 {
			for region, keys := range groups {
				c.regionTxnSize[region.id] = len(keys)
			}
		}
		sizeFunc = c.keyValueSize
	}
	// Make sure the group that contains primary key goes first.
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], sizeFunc, txnCommitBatchSize)
	delete(groups, firstRegion)
	for id, g := range groups {
		batches = appendBatchBySize(batches, id, g, sizeFunc, txnCommitBatchSize)
	}

	firstIsPrimary := bytes.Equal(keys[0], c.primary())
	_, actionIsCommit := action.(actionCommit)
	_, actionIsCleanup := action.(actionCleanup)
	if firstIsPrimary && (actionIsCommit || actionIsCleanup) {
		// primary should be committed/cleanup first
		err = c.doActionOnBatches(bo, action, batches[:1])
		if err != nil {
			return errors.Trace(err)
		}
		batches = batches[1:]
	}
	if actionIsCommit {
		// Commit secondary batches in background goroutine to reduce latency.
		// The backoffer instance is created outside of the goroutine to avoid
		// potential data race in unit test since `CommitMaxBackoff` will be updated
		// by test suites.
		secondaryBo := NewBackoffer(context.Background(), CommitMaxBackoff).WithVars(c.txn.vars)
		go func() {
			e := c.doActionOnBatches(secondaryBo, action, batches)
			if e != nil {
				logutil.BgLogger().Debug("2PC async doActionOnBatches",
					zap.Uint64("conn", c.connID),
					zap.Stringer("action type", action),
					zap.Error(e))
			}
		}()
	} else {
		err = c.doActionOnBatches(bo, action, batches)
	}
	return errors.Trace(err)
}

// doActionOnBatches does action to batches in parallel.
func (c *twoPhaseCommitter) doActionOnBatches(bo *Backoffer, action twoPhaseCommitAction, batches []batchKeys) error {
	if len(batches) == 0 {
		return nil
	}

	if len(batches) == 1 {
		e := action.handleSingleBatch(c, bo, batches[0])
		if e != nil {
			logutil.BgLogger().Debug("2PC doActionOnBatches failed",
				zap.Uint64("conn", c.connID),
				zap.Stringer("action type", action),
				zap.Error(e),
				zap.Uint64("txnStartTS", c.startTS))
		}
		return errors.Trace(e)
	}
	rateLim := len(batches)
	// Set rateLim here for the large transaction.
	// If the rate limit is too high, tikv will report service is busy.
	// If the rate limit is too low, we can't full utilize the tikv's throughput.
	// TODO: Find a self-adaptive way to control the rate limit here.
	if rateLim > 32 {
		rateLim = 32
	}
	batchExecutor := newBatchExecutor(rateLim, c, action, bo)
	err := batchExecutor.process(batches)
	return errors.Trace(err)
}

func (c *twoPhaseCommitter) keyValueSize(key []byte) int {
	size := len(key)
	if mutation := c.mutations[string(key)]; mutation != nil {
		size += len(mutation.Value)
	}
	return size
}

func (c *twoPhaseCommitter) keySize(key []byte) int {
	return len(key)
}

// You need to build the prewrite request in this function
// All keys in a batch are in the same region
func (c *twoPhaseCommitter) buildPrewriteRequest(batch batchKeys) *tikvrpc.Request {
	var req *pb.PrewriteRequest
	// Build the prewrite request from the input batch,
	// should use `twoPhaseCommitter.primary` to ensure that the primary key is not empty.
	// YOUR CODE HERE (lab3).
	//panic("YOUR CODE HERE")
	var mutations []*pb.Mutation
	// 遍历批次中的所有键
	for _, key := range batch.keys {
		// 为每个键创建mutation，操作类型为 Op_Put
		mutation := &pb.Mutation{
			Op:    pb.Op_Put,
			Key:   key,
			Value: c.mutations[string(key)].Value,
		}
		mutations = append(mutations, mutation)
	}

	req = &pb.PrewriteRequest{ 
		Mutations:    mutations,
		PrimaryLock:  c.primary(),
		StartVersion: c.startTS,
		LockTtl:      c.lockTTL,
	}
	return tikvrpc.NewRequest(tikvrpc.CmdPrewrite, req, pb.Context{})
}

// handleSingleBatch prewrites a batch of keys
func (actionPrewrite) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	req := c.buildPrewriteRequest(batch)
	for {
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			// The region info is read from region cache,
			// so the cache miss cases should be considered
			// You need to handle region errors here
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			// re-split keys and prewrite again.
			err = c.prewriteKeys(bo, batch.keys)
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		prewriteResp := resp.Resp.(*pb.PrewriteResponse)
		keyErrs := prewriteResp.GetErrors()
		if len(keyErrs) == 0 {
			return nil
		}
		var locks []*Lock
		for _, keyErr := range keyErrs {
			// Extract lock from key error
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return errors.Trace(err1)
			}
			logutil.BgLogger().Debug("prewrite encounters lock",
				zap.Uint64("conn", c.connID),
				zap.Stringer("lock", lock))
			locks = append(locks, lock)
		}
		// While prewriting, if there are some overlapped locks left by other transactions,
		// TiKV will return key errors. The statuses of these transactions are unclear.
		// ResolveLocks will check the transactions' statuses by locks and resolve them.
		// Set callerStartTS to 0 so as not to update minCommitTS.
		msBeforeExpired, _, err := c.store.lockResolver.ResolveLocks(bo, 0, locks)
		if err != nil {
			return errors.Trace(err)
		}
		if msBeforeExpired > 0 {
			err = bo.BackoffWithMaxSleep(BoTxnLock, int(msBeforeExpired), errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (c *twoPhaseCommitter) setUndeterminedErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.undeterminedErr = err
}

func (c *twoPhaseCommitter) getUndeterminedErr() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.undeterminedErr
}

func (actionCommit) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	// follow actionPrewrite.handleSingleBatch, build the commit request

	var resp *tikvrpc.Response
	var err error
	sender := NewRegionRequestSender(c.store.regionCache, c.store.client)
	// build and send the commit request
	// YOUR CODE HERE (lab3).
	//panic("YOUR CODE HERE")
	req := &pb.CommitRequest{
		StartVersion:  c.startTS,
		Keys:          batch.keys,
		CommitVersion: c.commitTS,
	}
	tikvReq := tikvrpc.NewRequest(tikvrpc.CmdCommit, req, pb.Context{})
	resp, err = sender.SendReq(bo, tikvReq, batch.region, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}
	logutil.BgLogger().Debug("actionCommit handleSingleBatch", zap.Bool("nil response", resp == nil))

	// If we fail to receive response for the request that commits primary key, it will be undetermined whether this
	// transaction has been successfully committed.
	// Under this circumstance,  we can not declare the commit is complete (may lead to data lost), nor can we throw
	// an error (may lead to the duplicated key error when upper level restarts the transaction). Currently the best
	// solution is to populate this error and let upper layer drop the connection to the corresponding mysql client.
	isPrimary := bytes.Equal(batch.keys[0], c.primary())
	if isPrimary && sender.rpcError != nil {
		c.setUndeterminedErr(errors.Trace(sender.rpcError))
	}

	failpoint.Inject("mockFailAfterPK", func() {
		if !isPrimary {
			err = errors.New("commit secondary keys error")
		}
	})
	if err != nil {
		return errors.Trace(err)
	}

	// handle the response and error refer to actionPrewrite.handleSingleBatch
	// YOUR CODE HERE (lab3).
	//panic("YOUR CODE HERE")
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		// The region info is read from region cache,
		// so the cache miss cases should be considered
		// You need to handle region errors here
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		err = c.commitKeys(bo, batch.keys)
		return errors.Trace(err)
	}
	if resp.Resp == nil {
		if isPrimary {
			c.setUndeterminedErr(errors.Trace(ErrBodyMissing))
		}
		return errors.Trace(ErrBodyMissing)
	}
	commitResp := resp.Resp.(*pb.CommitResponse)
	keyErr := commitResp.GetError()
	if keyErr != nil {
		c.mu.RLock()
		defer c.mu.RUnlock()
		err := extractKeyErr(keyErr)
		if c.mu.committed {
			logutil.BgLogger().Error("2PC failed commit key after primary key committed",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		logutil.BgLogger().Debug("2PC failed commit primary key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Group that contains primary key is always the first.
	// We mark transaction's status committed when we receive the first success response.
	c.mu.committed = true
	return nil
}

func (actionCleanup) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	// follow actionPrewrite.handleSingleBatch, build the rollback request

	// build and send the rollback request
	// YOUR CODE HERE (lab3).
	//panic("YOUR CODE HERE")
	var resp *tikvrpc.Response
	var err error
	sender := NewRegionRequestSender(c.store.regionCache, c.store.client)

	req := &pb.BatchRollbackRequest{
		StartVersion: c.startTS,
		Keys:         batch.keys,
	}
	tikvReq := tikvrpc.NewRequest(tikvrpc.CmdBatchRollback, req, pb.Context{})
	resp, err = sender.SendReq(bo, tikvReq, batch.region, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}

	logutil.BgLogger().Debug("actionCleanup handleSingleBatch", zap.Bool("nil response", resp == nil))
	// handle the response and error refer to actionPrewrite.handleSingleBatch
	// YOUR CODE HERE (lab3).
	//panic("YOUR CODE HERE")
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		// The region info is read from region cache,
		// so the cache miss cases should be considered
		// You need to handle region errors here
		// 可能出现因缓存过期而导致对应的存储节点返回 Region Error，此时需要分割 batch 后重试
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		// 重新rollback
		err = c.cleanupKeys(bo, batch.keys)
		return errors.Trace(err)
	}

	// 确保提交请求的响应有效
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cleanupResp := resp.Resp.(*pb.BatchRollbackResponse)
	keyErr := cleanupResp.GetError()
	// 提取响应中的错误信息
	if keyErr != nil {
		if keyErr.GetLocked() == nil {
			// 如果锁已经不存在，认为清理成功
			logutil.BgLogger().Debug("Lock not exist, cleanup considered successful",
				zap.Uint64("txnStartTS", c.startTS))
			return nil
		}
		c.mu.RLock()
		defer c.mu.RUnlock()
		err = errors.Errorf("conn %d 2PC cleanup failed: %s", c.connID, keyErr)
		logutil.BgLogger().Debug("2PC failed cleanup key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}
	return nil
}

func (c *twoPhaseCommitter) prewriteKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionPrewrite{}, keys)
}

func (c *twoPhaseCommitter) commitKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionCommit{}, keys)
}

func (c *twoPhaseCommitter) cleanupKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionCleanup{}, keys)
}

// execute executes the two-phase commit protocol.
// Prewrite phase:
//		1. Split keys by region -> batchKeys
// 		2. Prewrite all batches with transaction's start timestamp
// Commit phase:
//		1. Get the latest timestamp as commit ts
//      2. Check if the transaction can be committed(schema change during execution will fail the transaction)
//		3. Commit the primary key
//		4. Commit the secondary keys
// Cleanup phase:
//		When the transaction is unavailable to successfully committed,
//		transaction will fail and cleanup phase would start.
//		Cleanup phase will rollback a transaction.
// 		1. Cleanup primary key
// 		2. Cleanup secondary keys
func (c *twoPhaseCommitter) execute(ctx context.Context) (err error) {
	defer func() {
		// Always clean up all written keys if the txn does not commit.
		c.mu.RLock()
		committed := c.mu.committed
		undetermined := c.mu.undeterminedErr != nil
		c.mu.RUnlock()
		if !committed && !undetermined {
			c.cleanWg.Add(1)
			go func() {
				cleanupKeysCtx := context.WithValue(context.Background(), txnStartKey, ctx.Value(txnStartKey))
				cleanupBo := NewBackoffer(cleanupKeysCtx, cleanupMaxBackoff).WithVars(c.txn.vars)
				logutil.BgLogger().Debug("cleanupBo", zap.Bool("nil", cleanupBo == nil))
				// cleanup phase
				// YOUR CODE HERE (lab3).
				//panic("YOUR CODE HERE")
				err := c.cleanupKeys(cleanupBo, c.keys)
				if err != nil {
					logutil.Logger(ctx).Info("2PC cleanup failed",
						zap.Error(err),
						zap.Uint64("txnStartTS", c.startTS))
				} else {
					logutil.Logger(ctx).Info("2pc clean up done",
						zap.Uint64("txtStartTs", c.startTS))
				}

				c.cleanWg.Done()
			}()
		}
		c.txn.commitTS = c.commitTS
	}()

	// prewrite phase
	prewriteBo := NewBackoffer(ctx, PrewriteMaxBackoff).WithVars(c.txn.vars)
	logutil.BgLogger().Debug("prewriteBo", zap.Bool("nil", prewriteBo == nil))
	// YOUR CODE HERE (lab3).
	//panic("YOUR CODE HERE")
	err = c.prewriteKeys(prewriteBo, c.keys)
	if err != nil {
		logutil.Logger(ctx).Warn("2PC failed on prewrite",
			zap.Error(err),
			zap.Uint64("txtStartTs", c.startTS))
		return errors.Trace(err)
	}
	// commit phase
	commitTS, err := c.store.getTimestampWithRetry(NewBackoffer(ctx, tsoMaxBackoff).WithVars(c.txn.vars))
	if err != nil {
		logutil.Logger(ctx).Warn("2PC get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}

	// check commitTS
	if commitTS <= c.startTS {
		err = errors.Errorf("conn %d Invalid transaction tso with txnStartTS=%v while txnCommitTS=%v",
			c.connID, c.startTS, commitTS)
		logutil.BgLogger().Error("invalid transaction", zap.Error(err))
		return errors.Trace(err)
	}
	c.commitTS = commitTS
	if err = c.checkSchemaValid(); err != nil {
		return errors.Trace(err)
	}

	if c.store.oracle.IsExpired(c.startTS, kv.MaxTxnTimeUse) {
		err = errors.Errorf("conn %d txn takes too much time, txnStartTS: %d, comm: %d",
			c.connID, c.startTS, c.commitTS)
		return err
	}

	commitBo := NewBackoffer(ctx, CommitMaxBackoff).WithVars(c.txn.vars)
	logutil.BgLogger().Debug("commitBo", zap.Bool("nil", commitBo == nil))
	// Commit the transaction with `commitBo`.
	// If there is an error returned by commit operation, you should check if there is an undetermined error before return it.
	// Undetermined error should be returned if exists, and the database connection will be closed.
	// YOUR CODE HERE (lab3).
	//panic("YOUR CODE HERE")
	err = c.commitKeys(commitBo, c.keys)
	if err != nil {
		if undeterminedErr := c.getUndeterminedErr(); undeterminedErr != nil {
			logutil.Logger(ctx).Error("2PC commit result undetermined",
				zap.Error(err),
				zap.NamedError("rpcErr", undeterminedErr),
				zap.Uint64("txnStartTS", c.startTS))
			err = errors.Trace(terror.ErrResultUndetermined)
		}
		if !c.mu.committed {
			logutil.Logger(ctx).Error("2pc failed on commit",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		logutil.Logger(ctx).Debug("got some exceptions, but 2PC was still successful",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
	}
	return nil
}

type schemaLeaseChecker interface {
	Check(txnTS uint64) error
}

// checkSchemaValid checks if there are schema changes during the transaction execution(from startTS to commitTS).
// Schema change in a transaction is not allowed.
func (c *twoPhaseCommitter) checkSchemaValid() error {
	checker, ok := c.txn.us.GetOption(kv.SchemaChecker).(schemaLeaseChecker)
	if ok {
		err := checker.Check(c.commitTS)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
// Key+Value size below 16KB.
const txnCommitBatchSize = 16 * 1024

// batchKeys is a batch of keys in the same region.
type batchKeys struct {
	region RegionVerID
	keys   [][]byte
}

// appendBatchBySize appends keys to []batchKeys. It may split the keys to make
// sure each batch's size does not exceed the limit.
func appendBatchBySize(b []batchKeys, region RegionVerID, keys [][]byte, sizeFn func([]byte) int, limit int) []batchKeys {
	var start, end int
	for start = 0; start < len(keys); start = end {
		var size int
		for end = start; end < len(keys) && size < limit; end++ {
			size += sizeFn(keys[end])
		}
		b = append(b, batchKeys{
			region: region,
			keys:   keys[start:end],
		})
	}
	return b
}

// newBatchExecutor create processor to handle concurrent batch works(prewrite/commit etc)
func newBatchExecutor(rateLimit int, committer *twoPhaseCommitter,
	action twoPhaseCommitAction, backoffer *Backoffer) *batchExecutor {
	return &batchExecutor{rateLimit, nil, committer,
		action, backoffer, time.Duration(1 * time.Millisecond)}
}

// initUtils do initialize batchExecutor related policies like rateLimit util
func (batchExe *batchExecutor) initUtils() error {
	// init rateLimiter by injected rate limit number
	batchExe.rateLimiter = newRateLimit(batchExe.rateLim)
	return nil
}

// startWork concurrently do the work for each batch considering rate limit
func (batchExe *batchExecutor) startWorker(exitCh chan struct{}, ch chan error, batches []batchKeys) {
	for idx, batch1 := range batches {
		waitStart := time.Now()
		if exit := batchExe.rateLimiter.getToken(exitCh); !exit {
			batchExe.tokenWaitDuration += time.Since(waitStart)
			batch := batch1
			go func() {
				defer batchExe.rateLimiter.putToken()
				var singleBatchBackoffer *Backoffer
				if _, ok := batchExe.action.(actionCommit); ok {
					// Because the secondary batches of the commit actions are implemented to be
					// committed asynchronously in background goroutines, we should not
					// fork a child context and call cancel() while the foreground goroutine exits.
					// Otherwise the background goroutines will be canceled execeptionally.
					// Here we makes a new clone of the original backoffer for this goroutine
					// exclusively to avoid the data race when using the same backoffer
					// in concurrent goroutines.
					singleBatchBackoffer = batchExe.backoffer.Clone()
				} else {
					var singleBatchCancel context.CancelFunc
					singleBatchBackoffer, singleBatchCancel = batchExe.backoffer.Fork()
					defer singleBatchCancel()
				}
				ch <- batchExe.action.handleSingleBatch(batchExe.committer, singleBatchBackoffer, batch)
			}()
		} else {
			logutil.Logger(batchExe.backoffer.ctx).Info("break startWorker",
				zap.Stringer("action", batchExe.action), zap.Int("batch size", len(batches)),
				zap.Int("index", idx))
			break
		}
	}
}

// process will start worker routine and collect results
func (batchExe *batchExecutor) process(batches []batchKeys) error {
	var err error
	err = batchExe.initUtils()
	if err != nil {
		logutil.Logger(batchExe.backoffer.ctx).Error("batchExecutor initUtils failed", zap.Error(err))
		return err
	}

	// For prewrite, stop sending other requests after receiving first error.
	backoffer := batchExe.backoffer
	var cancel context.CancelFunc
	if _, ok := batchExe.action.(actionPrewrite); ok {
		backoffer, cancel = batchExe.backoffer.Fork()
		defer cancel()
	}
	// concurrently do the work for each batch.
	ch := make(chan error, len(batches))
	exitCh := make(chan struct{})
	go batchExe.startWorker(exitCh, ch, batches)
	// check results
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			logutil.Logger(backoffer.ctx).Debug("2PC doActionOnBatches failed",
				zap.Uint64("conn", batchExe.committer.connID),
				zap.Stringer("action type", batchExe.action),
				zap.Error(e),
				zap.Uint64("txnStartTS", batchExe.committer.startTS))
			// Cancel other requests and return the first error.
			if cancel != nil {
				logutil.Logger(backoffer.ctx).Debug("2PC doActionOnBatches to cancel other actions",
					zap.Uint64("conn", batchExe.committer.connID),
					zap.Stringer("action type", batchExe.action),
					zap.Uint64("txnStartTS", batchExe.committer.startTS))
				cancel()
			}
			if err == nil {
				err = e
			}
		}
	}
	close(exitCh)

	return err
}
