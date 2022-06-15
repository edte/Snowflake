package snowflake

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// 雪花算法一共 64 位，一般第一位不使用
// 分布式 ID 有很多计算方式，最简单的如直接搞一个 ID 节点，用来生成 ID
// 比如 Redis 的 incr、MySQL 的 ID 等等，都可以，但这必然会导致每次生成都要加全局锁
// 因此开销会比较大
// 另一种方式也可以使用 UUID 这种特殊形式的唯一 ID，不过由于 UUID 太长，导致平时传输等过程也很大
// 而 snowflake 就是一种类似于 UUID 的算法，可以在本地机器生成，而且全局唯一
// 雪花算法由机器 id+时间戳+顺序号组成，比较容易理解
// 时间戳就是带上时间，然后要根据机器进行唯一性标识，而一台机器上的则用顺序号（从 0 开始计数）区分

// 雪花算法有几个问题：
// 1. 时间回拨问题，由于雪花算法依赖于时间，如果机器的时间出了问题，则会导致生成的 id 重复
// 本项目的处理方式是，先睡眠 1s，然后再判断
// 2. workID 的分配问题，怎么分配 workID，以及怎么回收 workID 等等。这个方式就很广了，可以
// 直接取本机 IP、MAC 进行加工，或者用 MySQL、Redis 生成等等，比较常见的用可以使用 zk 来分配
// 回收，因此本库抽象了 WorkerID 这个函数，创建时可以自定义生成 workID 的方式，默认的
// 生成方式 defaultWorkerID，是通过取本机 IP，再通过简单运算得到的
// 3.机器 id 上限问题，由于使用 int64，故一共 64 位，一般的标准是第一位不用，然后其它位看情况分配
// 而根据具体的业务需要，可以自定义划分 time、work、seq 三个部分的位数，来解决机器上限等问题，
// 本库支持自定义分配位数
// 4. id 自增问题，一般使用雪花算法生成的 id 都是需要满足自增要求的，如果需要非自增，本库也提供了
// 支持，方法就是更换 workID 和 sequenceID 的分配，可以简单的打破自增

// 可参考：
// https://www.luozhiyun.com/archives/527
// https://tech.meituan.com/2017/04/21/mt-leaf.html
// https://www.jianshu.com/p/7680f88b990b
// https://github.com/bwmarrin/snowflake

const (
	// Epoch 起始时间
	epoch = int64(1577808000000)

	// 时间部分长度
	bitLenTime int64 = 41
	// 机器 id 部分长度
	bitLenWorkerID int64 = 63 - bitLenTime - bitLenSequence
	// 序列号部分长度
	bitLenSequence int64 = 10

	// 支持的最大序列 id 数量
	sequenceMask = int64(-1 ^ (-1 << bitLenSequence))
)

// WorkerID 生成 workID 的函数
type WorkerID func() (int64, error)

var (
	// defaultWorkerID IPv4 直接用 ip 进行简单运算得到 workerID
	defaultWorkerID WorkerID = func() (int64, error) {
		addr, err := net.InterfaceAddrs()
		if err != nil {
			return 0, err
		}

		var i net.IP

		for _, a := range addr {
			if ip, ok := a.(*net.IPNet); ok && !ip.IP.IsLoopback() {
				i = ip.IP.To4()
				break
			}
		}

		return (int64(i[2])<<8 + int64(i[3])) & 0x0fff, nil
	}
)

// Snowflake 雪花算法
type Snowflake struct {
	// 锁
	mutex sync.Mutex

	// 生成 workID 的函数
	w WorkerID

	// 非自增，换句话说，就是乱序，而默认为 false，则说明是自增
	// 如果设置了，则会更换 workerID 和 sequenceID 的位置
	nonIncrement bool

	// 默认配置
	// Epoch 起始时间
	epoch int64
	// 时间部分 bit 长度
	bitLenTime int64
	// workerID 部分 bit 长度
	bitLenWorkerID int64
	// 序列号部分 bit 长度
	bitLenSequence int64
	// 支持的最大序列 id 数量
	sequenceMask int64

	// id 快照
	// 上一次的时间
	lastTime int64
	// 时间部分
	time int64
	// 机器 id 部分
	workerID int64
	// 序列号部分
	sequenceID int64
}

// Option 可选配置
type Option func(s *Snowflake)

// WithEpoch 自定义初始时间 epoch
func WithEpoch(time int64) Option {
	return func(s *Snowflake) {
		s.epoch = time
	}
}

// WithWorkID 自定义 workID 生成方式
func WithWorkID(w WorkerID) Option {
	return func(s *Snowflake) {
		s.w = w
	}
}

// WithNonIncrement 自定义非自增
func WithNonIncrement() Option {
	return func(s *Snowflake) {
		s.nonIncrement = true
	}
}

// WithLen 自定义各部分长度
func WithLen(tl, wl, sl int64) Option {
	return func(s *Snowflake) {
		s.bitLenTime = tl
		s.bitLenWorkerID = wl
		s.bitLenSequence = sl
		s.sequenceMask = int64(-1 ^ (-1 << sl))
	}
}

// NewSnowflake 新建一个雪花算法
func NewSnowflake(opts ...Option) (*Snowflake, error) {
	// 默认配置
	s := &Snowflake{
		lastTime:       epoch,
		w:              defaultWorkerID,
		bitLenTime:     bitLenTime,
		bitLenWorkerID: bitLenWorkerID,
		bitLenSequence: bitLenSequence,
		sequenceMask:   sequenceMask,
		sequenceID:     0,
		nonIncrement:   false,
	}

	// 初始化自定义配置
	for i := range opts {
		opts[i](s)
	}

	// 设置 workerID
	wid, err := s.w()
	if err != nil {
		return nil, err
	}
	s.workerID = wid

	return s, nil
}

func (s *Snowflake) NextID() (id int64) {
	s.mutex.Lock()

	// 获取当前时间
	now := time.Now().UnixNano() / 1e6

	// 如果当前时间比上一次时间快
	// 则更新时间并且序列号初始化为 0
	if s.lastTime < now {
		s.lastTime = now
		s.sequenceID = 0
	} else if s.lastTime > now {
		// 如果当前时间比上一次时间慢，则说明时间出了问题（时间重拨），如果不处理，会导致 id 重复
		// 这里的处理方式是先等待一秒钟，再判断
		time.Sleep(time.Second)
		// 下面处理一样的，如果时间一样则序号增加，大于则更新时间，小于则报错
		now = time.Now().UnixNano() / 1e6
		if s.lastTime < now {
			s.lastTime = now
			s.sequenceID = 0
		} else if s.lastTime > now {
			log.Println("time error")
			return
		} else {
			s.sequenceID = (s.sequenceID + 1) & s.sequenceMask
			if s.sequenceID == 0 {
				now = time.Now().UnixNano() / 1e6
			}
		}
	} else {
		// 如果时间相同，则序列号自增
		// 注意达到最大值后需要重新从 0 开始
		s.sequenceID = (s.sequenceID + 1) & s.sequenceMask

		// 如果序列号变成 0，则说明序列号使用完了，所以需要更新时间，然后重新开始计算
		if s.sequenceID == 0 {
			now = time.Now().UnixNano() / 1e6
		}
	}

	// 获取时间部分
	s.time = now - s.epoch

	// 通过位运算生成结果
	// 结构为：
	//        time--work--sequence
	// 如果设置了 nonIncrement=true，则为
	//        time--sequence--work
	if !s.NonIncrementing() {
		id = s.time<<(s.bitLenWorkerID+s.bitLenSequence) | s.workerID<<s.bitLenWorkerID | s.sequenceID
	} else {
		id = s.time<<(s.bitLenWorkerID+s.bitLenSequence) | s.sequenceID<<s.bitLenSequence | s.workerID
	}

	s.mutex.Unlock()

	//fmt.Println()
	//fmt.Println(s.time, s.sequenceID, s.workerID)

	return
}

func (s *Snowflake) Time() int64 {
	return s.time
}

func (s *Snowflake) WorkerID() int64 {
	return s.workerID
}

func (s *Snowflake) SequenceID() int64 {
	return s.sequenceID
}

func (s *Snowflake) NonIncrementing() bool {
	return s.nonIncrement
}

func (s *Snowflake) Epoch() int64 {
	return s.epoch
}

func (s *Snowflake) BitLenTime() int64 {
	return s.bitLenTime
}

func (s *Snowflake) BitLenWorkerID() int64 {
	return s.bitLenWorkerID
}

func (s *Snowflake) BitLenSequence() int64 {
	return s.bitLenSequence
}

func (s *Snowflake) SequenceMask() int64 {
	return s.sequenceMask
}

func (s *Snowflake) LastTime() int64 {
	return s.lastTime
}

func (s *Snowflake) SetW(w WorkerID) {
	s.w = w
}

func (s *Snowflake) SetNonIncrementing(nonIncrementing bool) {
	s.nonIncrement = nonIncrementing
}

func (s *Snowflake) SetEpoch(epoch int64) {
	s.epoch = epoch
}

func (s *Snowflake) SetBitLenTime(bitLenTime int64) {
	s.bitLenTime = bitLenTime
}

func (s *Snowflake) SetBitLenWorkerID(bitLenWorkerID int64) {
	s.bitLenWorkerID = bitLenWorkerID
}

func (s *Snowflake) SetBitLenSequence(bitLenSequence int64) {
	s.bitLenSequence = bitLenSequence
}

func (s *Snowflake) SetLastTime(lastTime int64) {
	s.lastTime = lastTime
}

func (s *Snowflake) String() string {
	return fmt.Sprintf(`{"time":"%d","workd_id":"%d","sequenceID":"%d"}`, s.time, s.workerID, s.sequenceID)
}

// Parse 解析生成的 id 为各个部分
// 使用默认各个部分长度，默认自增分配
func Parse(id uint64) (time, workerID, sequenceID uint64) {
	const maskSequence = uint64((1<<bitLenSequence - 1) << bitLenWorkerID)
	const maskMachineID = uint64(1<<bitLenWorkerID - 1)

	time = id >> (bitLenSequence + bitLenWorkerID)
	workerID = id & maskSequence >> bitLenWorkerID
	sequenceID = id & maskMachineID

	return
}
