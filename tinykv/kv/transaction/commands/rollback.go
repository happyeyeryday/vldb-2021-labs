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

type Rollback struct {
	CommandBase
	request *kvrpcpb.BatchRollbackRequest
}

func NewRollback(request *kvrpcpb.BatchRollbackRequest) Rollback {
	return Rollback{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

func (r *Rollback) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	response := new(kvrpcpb.BatchRollbackResponse)

	for _, k := range r.request.Keys {
		resp, err := rollbackKey(k, txn, response)
		if resp != nil || err != nil {
			return resp, err
		}
	}
	return response, nil
}

// rollbackKey 执行单个键的回滚操作
// 这是事务回滚的核心逻辑，负责：
// 1. 检查锁的存在性和归属性
// 2. 处理各种异常状态（已提交、已回滚、锁丢失等）
// 3. 清理预写的数据和锁
// 4. 写入回滚记录，防止后续的提交操作
//
// 在 Percolator 协议中，回滚操作确保事务的原子性，
// 即使在网络分区或节点故障的情况下也能正确处理。
//
// 参数：
//   key: 要回滚的键
//   txn: MVCC 写事务
//   response: 响应对象，用于设置错误信息
// 返回：
//   interface{}: 如果需要提前返回响应则返回响应对象，否则为 nil
//   error: 系统级错误
func rollbackKey(key []byte, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
    // === 第一步：获取锁信息 ===
    // 检查目标键是否存在预写锁
    keyLock, lockError := txn.GetLock(key) 
    if lockError != nil {
        return nil, lockError
    }

    log.Info("rollbackKey",
        zap.Uint64("startTS", txn.StartTS),
        zap.String("key", hex.EncodeToString(key)))

    // === 第二步：处理锁异常情况 ===
    // 检查锁是否存在且属于当前事务
    if keyLock == nil || keyLock.Ts != txn.StartTS {
        // There is no lock, check the write status.
        // 锁不存在或不属于当前事务，需要检查写入状态
        
        // 查找当前事务的写入记录，返回写入记录和提交时间戳
        currentWrite, writeTimestamp, writeError := txn.CurrentWrite(key) 
        if writeError != nil {
            return nil, writeError
        }

        // Try to insert a rollback record if there's no correspond records, use `mvcc.WriteKindRollback` to represent
        // the type. Also the command could be stale that the record is already rolled back or committed.
        // If there is no write either, presumably the prewrite was lost. We insert a rollback write anyway.
        // if the key has already been rolled back, so nothing to do.
        // If the key has already been committed. This should not happen since the client should never send both
        // commit and rollback requests.
        // There is no write either, presumably the prewrite was lost. We insert a rollback write anyway.
        
        // 处理不同的写入状态：
        // 1. 无写入记录：可能预写丢失，需要插入回滚记录
        // 2. 已回滚：重复操作，直接返回成功
        // 3. 已提交：错误状态，不应该同时有提交和回滚
        
        if currentWrite == nil {
            // YOUR CODE HERE (lab2).
            // 情况1：没有找到写入记录，可能是预写操作丢失
            // 为了保证事务的一致性，需要插入回滚记录
            // 这样可以防止后续的提交操作成功执行
            rollbackWrite := mvcc.Write{
                StartTS: txn.StartTS, 
                Kind: mvcc.WriteKindRollback,
            }
            txn.PutWrite(key, txn.StartTS, &rollbackWrite)
            return nil, nil
            
        } else {
            if currentWrite.Kind == mvcc.WriteKindRollback {
                // 情况2：键已经被回滚，这是重复的回滚请求
                // 幂等操作，直接返回成功
                return nil, nil
            }

            // 情况3：键已经被提交，这是异常状态
            // 正常情况下客户端不应该同时发送提交和回滚请求
            abortError := new(kvrpcpb.KeyError)
            abortError.Abort = fmt.Sprintf("key has already been committed: %v at %d", key, writeTimestamp)
            
            // 使用反射设置响应对象的错误字段
            respValue := reflect.ValueOf(response)
            reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(abortError))
            return response, nil
        }
    }

    // === 第三步：执行正常回滚流程 ===
    // 锁存在且属于当前事务，执行回滚操作
    
    // 根据锁的类型决定是否需要删除数据
    // 如果是 Put 操作，需要删除预写的值
    if keyLock.Kind == mvcc.WriteKindPut {
        txn.DeleteValue(key)
    }
    // Delete 操作不需要额外的数据清理

    // 写入回滚记录
    // 这个记录标志着事务在该键上的最终状态是回滚
    rollbackWrite := mvcc.Write{
        StartTS: txn.StartTS, 
        Kind: mvcc.WriteKindRollback,
    }
    txn.PutWrite(key, txn.StartTS, &rollbackWrite)
    
    // 删除预写锁，释放资源
    // 此时其他事务可以访问该键
    txn.DeleteLock(key)

    // 回滚成功完成
    return nil, nil
}

func (r *Rollback) WillWrite() [][]byte {
	return r.request.Keys
}
