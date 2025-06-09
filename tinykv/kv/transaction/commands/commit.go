package commands

import (
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Commit struct {
	CommandBase
	request *kvrpcpb.CommitRequest
}

func NewCommit(request *kvrpcpb.CommitRequest) Commit {
	return Commit{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

// PrepareWrites 执行事务提交的准备工作
// 这是两阶段提交协议第二阶段的核心实现，负责：
// 1. 验证提交时间戳的有效性（必须大于事务开始时间戳）
// 2. 将所有预写的变更从锁定状态转换为已提交状态
// 3. 删除相应的锁，使数据对其他事务可见
// 4. 写入提交记录，标记事务的最终状态
//
// 在 Percolator 协议中，只有在所有键都成功预写后，
// 才会进入提交阶段，确保事务的原子性。
//
// 参数：
//   txn: MVCC 写事务，用于执行提交操作
// 返回：
//   interface{}: CommitResponse 对象，包含提交结果
//   error: 系统级错误（如果有）
func (c *Commit) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
    commitTimestamp := c.request.CommitVersion 

    // YOUR CODE HERE (lab2).
    // Check if the commitTs is invalid, the commitTs must be greater than the transaction startTs. If not
    // report unexpected error.
    
    // === 第一步：验证提交时间戳 ===
    // 在分布式事务中，时间戳的单调性是保证一致性的关键
    // 提交时间戳必须大于开始时间戳，这确保了：
    // 1. 事务的逻辑顺序正确性
    // 2. 快照隔离级别的正确实现
    // 3. 避免时间戳回退导致的数据不一致
    commitResponse := new(kvrpcpb.CommitResponse) 
    
    if commitTimestamp <= txn.StartTS {
        // 时间戳无效，构造错误响应
        // 这种情况通常表示调度器或客户端的时钟同步问题
        commitResponse.Error = &kvrpcpb.KeyError{
            Retryable: fmt.Sprintf("invalid commit timestamp %v for start timestamp %v", 
                commitTimestamp, txn.StartTS),
        }
        return commitResponse, nil
    }

    // === 第二步：逐个提交所有键 ===
    // 遍历请求中的所有键，将它们从预写状态转换为已提交状态
    // 这个过程包括：
    // - 验证锁的存在和归属
    // - 写入提交记录（Write 记录）
    // - 删除预写锁
    for _, targetKey := range c.request.Keys {
        // 调用 commitKey 函数处理单个键的提交
        // 如果任何键提交失败，整个提交操作将失败
        keyResponse, keyError := commitKey(targetKey, commitTimestamp, txn, commitResponse)
        
        // 检查是否有错误或需要返回特定响应
        if keyResponse != nil || keyError != nil {
            return commitResponse, keyError
        }
    }

    // === 第三步：返回成功响应 ===
    // 所有键都成功提交，事务正式完成
    // 此时数据对其他事务可见，事务的原子性得到保证
    return commitResponse, nil
}

// commitKey 执行单个键的提交操作
// 这是提交阶段的核心逻辑，负责：
// 1. 验证锁的存在性和归属性
// 2. 检查事务的历史状态（提交/回滚记录）
// 3. 将预写锁转换为提交记录
// 4. 释放锁，使数据对其他事务可见
//
// 参数：
//   key: 要提交的键
//   commitTs: 提交时间戳
//   txn: MVCC 写事务
//   response: 响应对象，用于设置错误信息
// 返回：
//   interface{}: 如果需要提前返回响应则返回响应对象，否则为 nil
//   error: 系统级错误
func commitKey(key []byte, commitTs uint64, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
    // === 第一步：获取锁信息 ===
    // 检查目标键是否存在预写锁
    keyLock, lockError := txn.GetLock(key) 
    if lockError != nil {
        return nil, lockError
    }

    log.Debug("commitKey", zap.Uint64("startTS", txn.StartTS),
        zap.Uint64("commitTs", commitTs),
        zap.String("key", hex.EncodeToString(key)))

    // === 第二步：处理锁异常情况 ===
    // 检查锁是否存在且属于当前事务
    if keyLock == nil || keyLock.Ts != txn.StartTS {
        // YOUR CODE HERE (lab2).
        // Key is locked by a different transaction, or there is no lock on the key. It's needed to
        // check the commit/rollback record for this key, if nothing is found report lock not found
        // error. Also the commit request could be stale that it's already committed or rolled back.
        
        // 锁异常的可能情况：
        // 1. 锁不存在：可能已被清理或事务已完成
        // 2. 锁属于其他事务：并发冲突或请求错误
        // 3. 重复请求：事务已经提交或回滚
        
        // 检查是否存在历史提交或回滚记录
        writeRecord, recordTimestamp, writeError := txn.MostRecentWrite(key)
        if writeError != nil {
            return nil, writeError
        }
        
        if writeRecord != nil {
            // 检查记录类型和归属
            if writeRecord.Kind != mvcc.WriteKindRollback && writeRecord.StartTS == txn.StartTS {
                // 情况1：当前事务已经提交过（重复请求）
                log.Warn("stale commit request", zap.Uint64("start_ts", txn.StartTS),
                    zap.String("key", hex.EncodeToString(key)),
                    zap.Uint64("commit_ts", recordTimestamp))
                return nil, nil // 幂等操作，直接返回成功
                
            } else if writeRecord.Kind == mvcc.WriteKindRollback && writeRecord.StartTS == txn.StartTS {
                // 情况2：当前事务已经回滚
                responseValue := reflect.ValueOf(response)
                rollbackError := &kvrpcpb.KeyError{ 
                    Retryable: fmt.Sprintf("Transaction with start_ts %v has been rolled back", txn.StartTS),
                }
                reflect.Indirect(responseValue).FieldByName("Error").Set(reflect.ValueOf(rollbackError))
                return response, nil
                
            } else {
                // 情况3：存在其他事务的提交记录，产生写冲突
                conflictError := &kvrpcpb.KeyError{
                    Retryable: fmt.Sprintf("Write conflict detected on key %s", string(key)),
                }
                log.Warn("write conflict detected", zap.Uint64("start_ts", txn.StartTS),
                    zap.String("key", hex.EncodeToString(key)),
                    zap.Uint64("conflict_ts", writeRecord.StartTS))
                return conflictError, nil
            }
        }

        // 情况4：没有找到任何相关记录，锁确实丢失
        responseValue := reflect.ValueOf(response)
        lockNotFoundError := &kvrpcpb.KeyError{
            Retryable: fmt.Sprintf("lock not found for key %v", key),
        }
        reflect.Indirect(responseValue).FieldByName("Error").Set(reflect.ValueOf(lockNotFoundError))
        return response, nil
    }

    // === 第三步：执行正常提交流程 ===
    // 锁存在且属于当前事务，可以安全提交
    
    // 创建提交记录（Write 记录）
    // 这个记录标志着数据的最终状态和可见性
    commitWrite := mvcc.Write{
        StartTS: txn.StartTS,      // 事务开始时间戳
        Kind:    keyLock.Kind,     // 操作类型（Put/Delete）
    }
    
    // 将提交记录写入存储
    // 使用提交时间戳作为版本号，确保数据的时间顺序
    txn.PutWrite(key, commitTs, &commitWrite)
    
    // 删除预写锁，释放资源
    // 此时数据对其他事务变为可见
    txn.DeleteLock(key)

    // 提交成功完成
    return nil, nil
}

func (c *Commit) WillWrite() [][]byte {
	return c.request.Keys
}
