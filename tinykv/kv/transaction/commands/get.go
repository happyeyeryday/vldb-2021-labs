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
    
    // 步骤1：检查目标键是否被其他事务锁定
    // 在 MVCC 中，读取前必须检查锁冲突，避免读到未提交的数据
    lock, err := txn.GetLock(key) // 获取指定键的锁信息
    if err != nil {
        return nil, nil, err
    }
    
    // 判断锁是否存在且会阻塞当前读取操作
    // IsLockedFor 检查锁的时间戳是否早于当前事务的开始时间
    if lock != nil && lock.IsLockedFor(key, g.startTs, response) {
        // 构造锁冲突错误响应
        // 客户端收到此错误后可以等待或重试
        response.Error = &kvrpcpb.KeyError{
            Locked: lock.Info(key), // 返回锁的详细信息
        }
        return response, nil, nil
    }

    // YOUR CODE HERE (lab2).
    // Search writes for a committed value, set results in the response.
    // Hint: Check the interfaces provided by `mvcc.RoTxn`.
    
    // 步骤2：读取已提交的数据版本
    // GetValue 会返回对当前事务可见的最新已提交版本
    value, err := txn.GetValue(key) // 从 MVCC 存储中获取键对应的值
    if err != nil {
        return nil, nil, err
    }
    
    // 根据读取结果设置响应
    if value == nil {
        response.NotFound = true // 键不存在或无可见版本
    } else {
        response.Value = value   // 返回读取到的值
    }

    return response, nil, nil
}
