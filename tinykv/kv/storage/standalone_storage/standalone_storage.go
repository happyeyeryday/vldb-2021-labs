package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage 是单节点 TinyKV 实例的存储实现
// 它不与其他节点通信，所有数据都存储在本地
// 使用 Badger 作为底层存储引擎
type StandAloneStorage struct {
	db *badger.DB // Badger 数据库实例
}

// NewStandAloneStorage 创建一个新的单机存储实例
// conf: 配置参数，包含数据目录等信息
// 返回: 初始化完成的 StandAloneStorage 实例
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// 使用 engine_util 创建名为 "kv" 的 Badger 数据库
	db := engine_util.CreateDB("kv", conf)
	return &StandAloneStorage{
		db: db,
	}
}

// Start 启动存储服务
// 对于单机存储，无需特殊的启动逻辑，直接返回 nil
// _: scheduler_client.Client 参数在单机模式下不需要
func (s *StandAloneStorage) Start(_ scheduler_client.Client) error {
	return nil
}

// Stop 停止存储服务并关闭数据库连接
// 返回: 关闭过程中的错误，如果有的话
func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

// Reader 创建一个存储读取器，用于读取数据
// ctx: RPC 上下文，包含事务信息等
// 返回: StorageReader 接口实例和可能的错误
// 需要实现：创建只读事务并返回 BadgerReader
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// YOUR CODE HERE (lab1).
	// 提示：
	// 1. 使用 s.db.NewTransaction(false) 创建只读事务
	// 2. 用 NewBadgerReader 包装事务并返回
	txn := s.db.NewTransaction(false)
	reader := NewBadgerReader(txn)
	return reader, nil
}

// Write 执行批量写操作
// 这是存储引擎的核心写入方法，负责：
// 1. 创建数据库事务来保证操作的原子性
// 2. 处理批量的 Put 和 Delete 操作
// 3. 将所有修改提交到底层存储
//
// 参数说明：
//   ctx: RPC 上下文信息，包含请求的元数据
//   batch: 要执行的修改操作列表，可能包含 Put 和 Delete 操作
// 返回值：
//   error: 写入过程中的错误，如果有的话
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
    // YOUR CODE HERE (lab1).
    // 提示：
    // 1. 检查 storage.Modify 的定义，了解 Put 和 Delete 操作
    // 2. 使用 badger 的事务接口执行批量操作
    // 3. 由于 badger 不直接支持列族，使用包装器来模拟
    // 4. 使用 engine_util.PutCF 和 engine_util.DeleteCF 方法

    // === 第一步：创建数据库事务 ===
    // 创建可写事务（参数 true 表示可写）
    // 事务确保所有操作要么全部成功，要么全部失败
    writeTxn := s.db.NewTransaction(true) 
    defer writeTxn.Discard() // 确保事务资源得到释放

    // === 第二步：处理批量修改操作 ===
    // 遍历所有的修改操作，支持 Put 和 Delete 两种类型
    for _, modification := range batch { 
        // 使用类型断言判断操作类型并执行相应的操作
        switch modification.Data.(type) {
        case storage.Put:
            // 处理 Put 操作：将键值对写入指定列族
            putOperation := modification.Data.(storage.Put)
            
            // 使用 KeyWithCF 将列族信息编码到键中
            // 这是因为 Badger 原生不支持列族，需要通过键前缀模拟
            operationError := writeTxn.Set(
                engine_util.KeyWithCF(putOperation.Cf, putOperation.Key), 
                putOperation.Value,
            )
            if operationError != nil {
                return operationError
            }
            
        case storage.Delete:
            // 处理 Delete 操作：从指定列族中删除键
            deleteOperation := modification.Data.(storage.Delete)
            
            // 同样使用 KeyWithCF 来处理列族信息
            operationError := writeTxn.Delete(
                engine_util.KeyWithCF(deleteOperation.Cf, deleteOperation.Key),
            )
            if operationError != nil {
                return operationError
            }
        }
    }

    // === 第三步：提交事务 ===
    // 将所有修改原子性地提交到存储引擎
    // 如果提交失败，所有操作都会被回滚
    if commitError := writeTxn.Commit(); commitError != nil {
        return commitError
    }

    return nil
}

// Client 返回调度器客户端
// 单机存储不需要调度器，返回 nil
func (s *StandAloneStorage) Client() scheduler_client.Client {
	return nil
}

// BadgerReader 是 StorageReader 接口的 Badger 实现
// 提供基于事务的读取功能，支持列族操作
type BadgerReader struct {
	txn *badger.Txn // Badger 事务，用于读取操作
}

// NewBadgerReader 创建一个新的 BadgerReader 实例
// txn: 用于读取的 Badger 事务
// 返回: 初始化的 BadgerReader 实例
func NewBadgerReader(txn *badger.Txn) *BadgerReader {
	return &BadgerReader{txn}
}

// GetCF 从指定列族中获取键对应的值
// cf: 列族名称（如 "default", "write", "lock" 等）
// key: 要查询的键
// 返回: 键对应的值和可能的错误
// 如果键不存在，返回 nil 而不是错误
func (b *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	// 使用 engine_util 从指定列族获取值
	val, err := engine_util.GetCFFromTxn(b.txn, cf, key)
	// 如果键不存在，返回 nil 而不是错误
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// IterCF 创建指定列族的迭代器
// cf: 列族名称
// 返回: 可以遍历该列族中所有键值对的迭代器
func (b *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, b.txn)
}

// Close 关闭读取器，释放相关资源
// 丢弃底层事务，释放内存
func (b *BadgerReader) Close() {
	b.txn.Discard()
}
