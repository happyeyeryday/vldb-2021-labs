package commands

import (
	"encoding/hex"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Prewrite represents the prewrite stage of a transaction. A prewrite contains all writes (but not reads) in a transaction,
// if the whole transaction can be written to underlying storage atomically and without conflicting with other
// transactions (complete or in-progress) then success is returned to the client. If all a client's prewrites succeed,
// then it will send a commit message. I.e., prewrite is the first phase in a two phase commit.
type Prewrite struct {
	CommandBase
	request *kvrpcpb.PrewriteRequest
}

func NewPrewrite(request *kvrpcpb.PrewriteRequest) Prewrite {
	return Prewrite{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

// PrepareWrites prepares the data to be written to the raftstore. The data flow is as follows.
// The tinysql part:
// 		user client -> insert/delete query -> tinysql server
//      query -> parser -> planner -> executor -> the transaction memory buffer
//		memory buffer -> kv muations -> kv client 2pc committer
//		committer -> prewrite all the keys
//		committer -> commit all the keys
//		tinysql server -> respond to the user client
// The tinykv part:
//		prewrite requests -> transaction mutations -> raft request
//		raft req -> raft router -> raft worker -> peer propose raft req
//		raft worker -> peer receive majority response for the propose raft req  -> peer raft committed entries
//  	raft worker -> process committed entries -> send apply req to apply worker
//		apply worker -> apply the correspond requests to storage(the state machine) -> callback
//		callback -> signal the response action -> response to kv client
func (p *Prewrite) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	response := new(kvrpcpb.PrewriteResponse)

	// Prewrite all mutations in the request.
	for _, m := range p.request.Mutations {
		keyError, err := p.prewriteMutation(txn, m)
		if keyError != nil {
			response.Errors = append(response.Errors, keyError)
		} else if err != nil {
			return nil, err
		}
	}

	return response, nil
}

// prewriteMutation 执行单个变更操作的预写处理
// 这是两阶段提交协议第一阶段的核心实现，负责：
// 1. 检查写写冲突：确保没有其他事务在当前事务开始后修改了同一个键
// 2. 检查锁冲突：确保目标键没有被其他事务锁定
// 3. 创建预写锁：为当前操作创建锁，防止其他事务并发修改
// 4. 写入数据：将新值写入 MVCC 存储（如果是 Put 操作）
//
// 返回值说明：
//   - (nil, nil): 预写成功
//   - (keyError, nil): 键相关错误（锁冲突或写冲突）
//   - (nil, systemError): 系统内部错误
//
// 参数：
//   txn: MVCC 写事务，提供写入和冲突检测能力
//   mut: 要执行的变更操作（Put 或 Delete）
func (p *Prewrite) prewriteMutation(txn *mvcc.MvccTxn, mut *kvrpcpb.Mutation) (*kvrpcpb.KeyError, error) {
    mutationKey := mut.Key 

    log.Debug("prewrite key", zap.Uint64("start_ts", txn.StartTS),
        zap.String("key", hex.EncodeToString(mutationKey)))

    // YOUR CODE HERE (lab2).
    // Check for write conflicts.
    // Hint: Check the interafaces provided by `mvcc.MvccTxn`. The error type `kvrpcpb.WriteConflict` is used
    //		 denote to write conflict error, try to set error information properly in the `kvrpcpb.KeyError`
    //		 response.
    
    // === 第一步：检查写写冲突 ===
    // 写写冲突检测是保证 Snapshot Isolation 的关键
    // 如果目标键在当前事务开始后被其他事务修改，则存在冲突
    latestWrite, conflictTimestamp, writeError := txn.MostRecentWrite(mutationKey)
    if writeError != nil {
        return nil, writeError
    }
    
    // 检查是否存在写写冲突
    // 冲突条件：最新提交版本的时间戳 >= 当前事务的开始时间戳
    if latestWrite != nil && conflictTimestamp >= txn.StartTS {
        // 构造写冲突错误响应
        conflictError := &kvrpcpb.KeyError{ 
            Conflict: &kvrpcpb.WriteConflict{
                StartTs:    txn.StartTS,           // 当前事务的开始时间戳
                ConflictTs: conflictTimestamp,     // 冲突的提交时间戳
                Key:        mutationKey,           // 冲突的键
                Primary:    p.request.PrimaryLock, // 当前事务的主键
            },
        }
        return conflictError, nil
    }

    // YOUR CODE HERE (lab2).
    // Check if key is locked. Report key is locked error if lock does exist, note the key could be locked
    // by this transaction already and the current prewrite request is stale.
    
    // === 第二步：检查锁冲突 ===
    // 锁冲突检测确保同一时刻只有一个事务能修改某个键
    existingLock, lockError := txn.GetLock(mutationKey)
    if lockError != nil {
        return nil, lockError
    }
    
    if existingLock != nil {
        // 检查锁的归属
        if existingLock.Ts != txn.StartTS {
            // 情况1：锁属于其他事务，返回锁冲突错误
            lockConflictError := &kvrpcpb.KeyError{
                Locked: existingLock.Info(mutationKey), // 返回锁的详细信息
            }
            return lockConflictError, nil
        } else {
            // 情况2：锁属于当前事务，可能是重复请求
            // 直接返回成功，避免重复操作
            return nil, nil
        }
    }

    // YOUR CODE HERE (lab2).
    // Write a lock and value.
    // Hint: Check the interfaces provided by `mvccTxn.Txn`.
    
    // === 第三步：创建预写锁和写入数据 ===
    // 在没有冲突的情况下，为当前操作创建锁并写入数据
    
    // 创建预写锁
    // 锁包含了事务的关键信息，用于后续的提交或回滚
    prewriteLock := &mvcc.Lock{
        Primary: p.request.PrimaryLock,            // 事务的主键（用于协调）
        Ts:      txn.StartTS,                     // 事务的开始时间戳
        Ttl:     p.request.LockTtl,               // 锁的生存时间
        Kind:    mvcc.WriteKindFromProto(mut.Op), // 操作类型（Put/Delete）
    }
    
    // 将锁写入存储
    txn.PutLock(mutationKey, prewriteLock)
    
    // 根据操作类型写入或删除数据
    switch mut.Op {
    case kvrpcpb.Op_Put:
        // Put 操作：写入新值到 MVCC 存储
        // 数据以 (key, start_ts) -> value 的形式存储
        txn.PutValue(mutationKey, mut.Value)
        
    case kvrpcpb.Op_Del:
        // Delete 操作：标记键为删除状态
        // 在 MVCC 中，删除是通过写入删除标记实现的
        txn.DeleteValue(mutationKey)
    }

    // 预写成功完成
    return nil, nil
}

func (p *Prewrite) WillWrite() [][]byte {
	result := [][]byte{}
	for _, m := range p.request.Mutations {
		result = append(result, m.Key)
	}
	return result
}
