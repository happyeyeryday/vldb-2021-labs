package commands

import (
	"encoding/hex"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ResolveLock struct {
	CommandBase
	request  *kvrpcpb.ResolveLockRequest
	keyLocks []mvcc.KlPair
}

func NewResolveLock(request *kvrpcpb.ResolveLockRequest) ResolveLock {
	return ResolveLock{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

// PrepareWrites 执行锁解析的准备工作
// 这是 Percolator 协议中锁解析的核心实现，负责：
// 1. 根据事务的最终状态（提交/回滚）批量解析锁
// 2. 清理过期或冲突的锁，恢复系统的正常运行
// 3. 确保数据的一致性和可用性
// 4. 处理分布式环境下的锁冲突和死锁问题
//
// 锁解析通常在以下场景中使用：
// - 清理崩溃事务遗留的锁
// - 解决事务间的锁冲突
// - 处理网络分区后的锁状态恢复
// - 客户端主动清理自己的锁
//
// 参数：
//   txn: MVCC 写事务，用于执行锁解析操作
// 返回：
//   interface{}: ResolveLockResponse 对象，包含解析结果
//   error: 系统级错误
func (rl *ResolveLock) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
    // === 事务状态映射说明 ===
    // A map from start timestamps to commit timestamps which tells us whether a transaction (identified by start ts)
    // has been committed (and if so, then its commit ts) or rolled back (in which case the commit ts is 0).
    
    // 这是一个从事务开始时间戳到提交时间戳的映射，用于表示每个事务的最终状态：
    // - 如果事务已提交：映射记录开始时间戳与对应的提交时间戳
    // - 如果事务已回滚：提交时间戳设置为 0，表示事务被回滚
    
    // === 第一步：获取事务状态信息 ===
    // 从请求中获取目标事务的提交版本
    // commitVersion > 0 表示事务已提交，= 0 表示事务需要回滚
    transactionCommitTs := rl.request.CommitVersion 
    resolveResponse := new(kvrpcpb.ResolveLockResponse) 

    log.Info("There keys to resolve",
        zap.Uint64("lockTS", txn.StartTS),
        zap.Int("number", len(rl.keyLocks)),
        zap.Uint64("commit_ts", transactionCommitTs))

    // === 第二步：批量处理所有相关的锁 ===
    // 遍历在 Read 阶段收集到的所有属于目标事务的锁
    // 根据事务的最终状态决定提交还是回滚每个锁
    for _, keyLockPair := range rl.keyLocks {
        // YOUR CODE HERE (lab2).
        // Try to commit the key if the transaction is committed already, or try to rollback the key if it's not.
        // The `commitKey` and `rollbackKey` functions could be useful.
        
        log.Debug("resolve key", zap.String("key", hex.EncodeToString(keyLockPair.Key)))
        
        // 根据事务状态执行相应的操作
        if transactionCommitTs > 0 {
            // === 情况1：事务已提交，需要提交所有相关的键 ===
            // 调用 commitKey 将预写锁转换为提交记录
            // 这使得数据对其他事务可见
            _, commitError := commitKey(keyLockPair.Key, transactionCommitTs, txn, resolveResponse)
            if commitError != nil {
                return nil, commitError
            }
            
        } else {
            // === 情况2：事务需要回滚，清理所有相关的键 ===
            // 调用 rollbackKey 删除预写锁和数据
            // 这确保事务的原子性，所有操作要么全部成功，要么全部失败
            _, rollbackError := rollbackKey(keyLockPair.Key, txn, resolveResponse)
            if rollbackError != nil {
                return nil, rollbackError
            }
        }
    }

    // === 第三步：返回解析结果 ===
    // 所有锁都已成功解析，系统恢复正常状态
    return resolveResponse, nil
}

func (rl *ResolveLock) WillWrite() [][]byte {
	return nil
}

func (rl *ResolveLock) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	// Find all locks where the lock's transaction (start ts) is in txnStatus.
	txn.StartTS = rl.request.StartVersion
	keyLocks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, nil, err
	}
	rl.keyLocks = keyLocks
	keys := [][]byte{}
	for _, kl := range keyLocks {
		keys = append(keys, kl.Key)
	}
	return nil, keys, nil
}
