package commands

import (
	"encoding/hex"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Get struct {
	ReadOnly
	CommandBase
	request *kvrpcpb.GetRequest
}

func NewGet(request *kvrpcpb.GetRequest) Get {
	return Get{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.Version,
		},
		request: request,
	}
}

// 从数据库中读取数据
func (g *Get) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	// 从 Get 命令的请求数据中获取 key，tinykv/proto/pkg/kvrpcpb/kvrpcpb.pb.go
	key := g.request.Key

	// 记录日志，包括事务的开始时间戳和要读取的键
	log.Debug("read key", zap.Uint64("start_ts", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))

	// 创建一个新的 kvrpcpb.GetResponse 类型的指针，用于存储 Get 请求的响应
	// tinykv/proto/pkg/kvrpcpb/kvrpcpb.pb.go
	response := new(kvrpcpb.GetResponse)

	// panic("kv get is not implemented yet")
	// YOUR CODE HERE (lab2).
	// Check for locks and their visibilities.
	// Hint: Check the interfaces provided by `mvcc.RoTxn`.
	lock, err := txn.GetLock(key) // tinykv/kv/transaction/mvcc/transaction.go
	if err != nil {
		return nil, nil, err
	}
	// 存在锁且指定的键在给定的事务开始时间戳被锁定
	// if lock != nil && lock.Ts < txn.StartTS {	// tinykv/kv/transaction/mvcc/lock.go
	if lock != nil && lock.IsLockedFor(key, g.startTs, response) {
		// 错误类型是一个 KeyError，说明当前键的读取操作因锁冲突而失败
		// kvrpcpb.GetResponse 的 Error 类型是 *KeyError（type KeyError struct定义）
		response.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(key),
		}
		return response, nil, nil
	}

	// YOUR CODE HERE (lab2).
	// Search writes for a committed value, set results in the response.
	// Hint: Check the interfaces provided by `mvcc.RoTxn`.
	// tinykv/kv/transaction/mvcc/transaction.go
	value, err := txn.GetValue(key)
	if err != nil {
		return nil, nil, err
	}
	if value == nil {
		response.NotFound = true
	} else {
		response.Value = value
	}

	return response, nil, nil

}
