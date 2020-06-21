package pool

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// data Entry a = Entry {
//        entry :: a
//      , lastUse :: UTCTime
//      -- ^ Time of last return.
//      }
type Entry struct {
	entry   interface{}
	lastUse time.Time
}

func (e Entry) String() string {
	return fmt.Sprintf("{ entry:%v, lastUse: %s }", e.entry, e.lastUse.Format("2006-01-02 15:04:05"))
}

type LocalPool struct {
	inUse   int
	entries []Entry
	lock    *sync.Mutex
}

func (lp LocalPool) String() string {
	return fmt.Sprintf("{ inUse:%d entries:%v }", lp.inUse, lp.entries)
}

func (lp *LocalPool) lockWithModify(modifyFunc func(int, []Entry) (int, []Entry, error)) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()

	inUse, entries, err := modifyFunc(lp.inUse, lp.entries)

	if err != nil {
		return err
	}
	lp.inUse = inUse
	lp.entries = entries
	return nil
}

//参考Haskell的resource-pool实现的pool
type Pool struct {
	//创建资源的方法
	createFunc func() (interface{}, error)
	//关闭资源的方法
	destoryFunc func(interface{}) error
	//子池数
	numStripes int32
	//闲置的资源持续时间, 默认0.5秒
	idleTime time.Duration
	//最大资源
	maxResource int
	//子池
	localPools []*LocalPool
	//循环清理资源池协程的取消函数
	reaperCancel context.CancelFunc
}

func (p Pool) ShowPool() string {
	return fmt.Sprintf("{ localPools: %v, stripes: %d, maxResource: %d}", p.localPools, p.numStripes, p.maxResource)
}

//CreatePool 创建资源池
func CreatePool(
	createFunc func() (interface{}, error),
	destoryFunc func(interface{}) error,
	subPools int32,
	idleTime time.Duration,
	maxResource int,
) (*Pool, error) {
	if subPools < 1 {
		return nil, fmt.Errorf("stripe count must more than 1, current: %d", subPools)
	}

	if idleTime.Milliseconds() < 500 {
		return nil, fmt.Errorf("idle time must more than 0.5 sec, current: %s", idleTime)
	}

	if maxResource < 1 {
		return nil, fmt.Errorf("maxResource must more than 1, current: %d", maxResource)
	}

	//初始化子池
	localPools := make([]*LocalPool, subPools)
	for i := range localPools {
		localPools[i] = &LocalPool{inUse: 0, entries: []Entry{}, lock: &sync.Mutex{}}
	}

	ctx, cancel := context.WithCancel(context.Background())
	go createAndStartReaper(ctx, idleTime, localPools, destoryFunc)

	pool := Pool{createFunc, destoryFunc, subPools, idleTime, maxResource, localPools, cancel}
	return &pool, nil
}

//循环处理资源池上下文
func createAndStartReaper(ctx context.Context, idleTime time.Duration, localPools []*LocalPool, destoryFunc func(interface{}) error) {
	flag := true
	for flag {
		select {
		case <-ctx.Done():
			log.Println("context done, check loop stopped.")
			flag = false
		default:
			time.Sleep(500 * time.Millisecond)
			now := time.Now()
			for _, localPool := range localPools {
				localPool.lockWithModify(func(inUse int, entries []Entry) (int, []Entry, error) {
					stales := []Entry{}
					unstales := []Entry{}
					for _, entry := range localPool.entries {
						if isStale(idleTime, entry.lastUse, now) {
							stales = append(stales, entry)
						} else {
							unstales = append(unstales, entry)
						}
					}

					//关闭过期的资源
					var err error
					for _, entry := range stales {
						err = destoryFunc(entry.entry)
					}
					if err != nil {
						return 0, nil, err
					} else {
						return (inUse - len(stales)), unstales, nil
					}
				})
			}
		}
	}
}

//判断是否过期
func isStale(idleTime time.Duration, lastUse time.Time, now time.Time) bool {
	return (now.UnixNano()/1e6)-(lastUse.UnixNano()/1e6) > idleTime.Milliseconds()
}

//创建、获取内部的子池
//因为协程不能拿到id，所以这里用随机数获取
func (p *Pool) getLocalPool() *LocalPool {
	index := rand.Int31n(p.numStripes)
	localPool := p.localPools[index]
	return localPool
}

//TakeResource 从资源池里获取资源
func (p *Pool) TakeResource() (interface{}, *LocalPool, error) {
	local := p.getLocalPool()
	var entry interface{}
	var err error

	flag := false
	for !flag {
		local.lockWithModify(func(inUse int, entries []Entry) (int, []Entry, error) {
			if len(entries) != 0 {
				//资源池里有东西，直接返回
				entry = entries[0].entry
				flag = true
				return inUse, entries[1:], nil
			} else {
				//资源池里没有东西，看看能否创建，创建一个
				if inUse == p.maxResource {
					return inUse, entries, nil
				} else {
					entry, err = p.createFunc()
					flag = true
					if err != nil {
						return inUse, entries, err
					} else {
						return inUse + 1, entries, nil
					}
				}
			}
		})
		if !flag {
			time.Sleep(500 * time.Millisecond)
		}
	}

	if err != nil {
		return nil, nil, err
	} else {
		return entry, local, nil
	}
}

//PutResource 将资源放回资源池
func (p *Pool) PutResource(local *LocalPool, resource interface{}) {
	now := time.Now()
	entry := Entry{entry: resource, lastUse: now}
	if resource != nil {
		local.lockWithModify(func(inUse int, entries []Entry) (int, []Entry, error) {
			entries = append(entries, entry)
			return inUse, entries, nil
		})
	}
}

//WithResource 直接利用资源池里的资源干活
func (p *Pool) WithResource(doFunc func(interface{}) (interface{}, error)) (interface{}, error) {
	resource, local, err := p.TakeResource()
	if err != nil {
		return nil, err
	}
	defer func() {
		p.PutResource(local, resource)
	}()

	result, err := doFunc(resource)
	return result, err
}

//Close 关闭资源池
func (p *Pool) Close() error {
	p.reaperCancel()
	var err error
	for _, localPool := range p.localPools {
		localPool.lockWithModify(func(inUse int, entries []Entry) (int, []Entry, error) {
			for _, entry := range entries {
				err = p.destoryFunc(entry.entry)
			}
			return 0, []Entry{}, err
		})
	}

	return err
}
