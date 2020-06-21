package pool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

//test
type MyResource struct {
	Name string
	Uses int
	Lock chan struct{}
}

func (m *MyResource) String() string {
	return fmt.Sprintf("{ Name:%s Uses:%d }", m.Name, m.Uses)
}

func (m *MyResource) inc() {
	<-m.Lock
	m.Uses += 1
	m.Lock <- struct{}{}
}

func NewResource(name string) *MyResource {
	resource := MyResource{name, 0, make(chan struct{}, 1)}
	resource.Lock <- struct{}{}
	return &resource
}

func TestResource(t *testing.T) {
	idx := make(chan int, 1)
	idx <- 0

	uses := make(chan int, 1)
	uses <- 0

	invokes := make(chan int, 1)
	invokes <- 0

	concurrent := 100
	wg := &sync.WaitGroup{}
	wg.Add(concurrent)

	pool, _ := CreatePool(
		func() (interface{}, error) {
			index := <-idx
			name := fmt.Sprint("Name_", index)
			idx <- index + 1
			my := NewResource(name)
			t.Log(my.Name, " create at: ", time.Now().Format("2006-01-02 15:04:05"))
			return my, nil
		},
		func(v interface{}) error {
			my := v.(*MyResource)
			allUses := <-uses
			uses <- allUses + my.Uses
			t.Log(my.Name, " close at: ", time.Now().Format("2006-01-02 15:04:05"), " and it uses: ", my.Uses)

			for i := 0; i < my.Uses; i++ {
				wg.Done()
			}

			return nil
		},
		3, 500*time.Millisecond, 20)
	defer pool.Close()

	//开始并发执行
	for i := 0; i < concurrent; i++ {
		go func(n int) {
			pool.WithResource(func(v interface{}) (interface{}, error) {
				ink := <-invokes
				invokes <- ink + 1
				my := v.(*MyResource)
				t.Log("goroutine_", n, " invoking ", my.Name, " at: ", time.Now().Format("2006-01-02 15:04:05"))
				my.inc()
				return nil, nil
			})
		}(i)
	}
	wg.Wait()
	t.Log("invokers: ", <-invokes)
	t.Log("uses: ", <-uses)
}
