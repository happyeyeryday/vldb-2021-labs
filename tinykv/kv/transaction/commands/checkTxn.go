package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type CheckTxnStatus struct {
	CommandBase
	request *kvrpcpb.CheckTxnStatusRequest
}

func NewCheckTxnStatus(request *kvrpcpb.CheckTxnStatusRequest) CheckTxnStatus {
	return CheckTxnStatus{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.LockTs,
		},
		request: request,
	}
}

// PrepareWrites 执行事务状态检查的准备工作
// 这是 Percolator 协议中事务状态检查的核心实现，负责：
// 1. 检查主键锁的状态和有效性
// 2. 处理锁过期的情况（自动回滚）
// 3. 检查事务的历史记录（提交/回滚状态）
// 4. 确保事务状态的一致性和可靠性
//
// 事务状态检查通常用于：
// - 清理过期的锁（避免死锁）
// - 检测事务是否已完成
// - 为冲突解决提供状态信息
//
// 参数：
//   txn: MVCC 写事务，用于检查和修改事务状态
// 返回：
//   interface{}: CheckTxnStatusResponse 对象，包含检查结果和操作类型
//   error: 系统级错误
func (c *CheckTxnStatus) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
    primaryKey := c.request.PrimaryKey 
    statusResponse := new(kvrpcpb.CheckTxnStatusResponse) 

    // === 第一步：获取主键锁信息 ===
    // 检查事务的主键是否存在锁
    // 主键锁的状态决定了整个事务的状态
    primaryLock, lockError := txn.GetLock(primaryKey)
    if lockError != nil {
        return nil, lockError
    }

    // === 第二步：处理锁存在的情况 ===
    // 如果锁存在且属于当前事务，需要检查锁是否过期
    if primaryLock != nil && primaryLock.Ts == txn.StartTS {
        // 检查锁是否已过期
        // 锁过期条件：锁的物理时间 + TTL < 当前物理时间
        if physical(primaryLock.Ts)+primaryLock.Ttl < physical(c.request.CurrentTs) {
            // YOUR CODE HERE (lab2).
            // Lock has expired, try to rollback it. `mvcc.WriteKindRollback` could be used to
            // represent the type. Try using the interfaces provided by `mvcc.MvccTxn`.
            
            // 锁已过期，执行自动回滚操作
            // 这是防止死锁的重要机制
            log.Info("checkTxnStatus rollback the primary lock as it's expired",
                zap.Uint64("lock.TS", primaryLock.Ts),
                zap.Uint64("lock.Ttl", primaryLock.Ttl),
                zap.Uint64("physical(lock.TS)", physical(primaryLock.Ts)),
                zap.Uint64("txn.StartTS", txn.StartTS),
                zap.Uint64("currentTS", c.request.CurrentTs),
                zap.Uint64("physical(currentTS)", physical(c.request.CurrentTs)))

            // 创建回滚记录，标记事务失败
            expiredRollbackWrite := mvcc.Write{
                StartTS: primaryLock.Ts, 
                Kind: mvcc.WriteKindRollback,
            }
            txn.PutWrite(primaryKey, primaryLock.Ts, &expiredRollbackWrite)
            
            // 删除过期的锁
            txn.DeleteLock(primaryKey)
            
            // 如果是 Put 操作，还需要删除预写的值
            if primaryLock.Kind == mvcc.WriteKindPut {
                txn.DeleteValue(primaryKey)
            }
            
            // 设置响应：锁过期回滚
            statusResponse.Action = kvrpcpb.Action_TTLExpireRollback
            
        } else {
            // 锁未过期，保持现状
            // 返回锁的剩余生存时间，供客户端参考
            statusResponse.Action = kvrpcpb.Action_NoAction
            statusResponse.LockTtl = primaryLock.Ttl
        }

        return statusResponse, nil
    }

    // === 第三步：处理锁不存在或不属于当前事务的情况 ===
    // 需要检查写入记录来确定事务的最终状态
    // 可能的情况：
    // 1. 事务从未执行预写操作
    // 2. 事务已经完成（提交或回滚）
    // 3. 锁被其他操作清理
    
    // 检查当前事务的写入记录
    currentWrite, commitTimestamp, writeError := txn.CurrentWrite(primaryKey)
    if writeError != nil {
        return nil, writeError
    }

    if currentWrite == nil {
        // YOUR CODE HERE (lab2).
        // The lock never existed, it's still needed to put a rollback record on it so that
        // the stale transaction commands such as prewrite on the key will fail.
        // Note try to set correct `response.Action`,
        // the action types could be found in kvrpcpb.Action_xxx.
        
        // 情况1：没有找到任何记录
        // 可能原因：
        // - 预写操作失败或从未执行
        // - 锁被意外清理
        // - 客户端发起了无效请求
        
        // 插入回滚记录以确保一致性
        // 这样可以防止后续的过期预写请求成功执行
        missingLockRollbackWrite := mvcc.Write{
            StartTS: c.request.LockTs, 
            Kind: mvcc.WriteKindRollback,
        }
        txn.PutWrite(primaryKey, c.request.LockTs, &missingLockRollbackWrite)
        
        // 设置响应：锁不存在，执行回滚
        statusResponse.Action = kvrpcpb.Action_LockNotExistRollback
        return statusResponse, nil
    }

    if currentWrite.Kind == mvcc.WriteKindRollback {
        // 情况2：事务已经回滚
        // 无需进一步操作，返回无动作状态
        statusResponse.Action = kvrpcpb.Action_NoAction
        return statusResponse, nil
    }

    // 情况3：事务已经提交
    // 返回提交时间戳和无动作状态
    statusResponse.CommitVersion = commitTimestamp
    statusResponse.Action = kvrpcpb.Action_NoAction
    return statusResponse, nil
}

// physical 用于从全局时间戳中提取物理时间部分
func physical(ts uint64) uint64 {
    return ts >> tsoutil.PhysicalShiftBits
}

// WillWrite 返回本次命令会写入的 key（用于调度器优化）
func (c *CheckTxnStatus) WillWrite() [][]byte {
    return [][]byte{c.request.PrimaryKey}
}
