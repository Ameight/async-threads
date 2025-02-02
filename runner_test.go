package async_process

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

type someStruct struct {
	process1 bool
	process2 bool
	process3 bool
	process4 bool
	process5 bool
}

type asyncProcessStruct struct {
	lock           *sync.Mutex
	someSafeStruct *someStruct
}

func (aps *asyncProcessStruct) getLocalCopy() someStruct {
	aps.lock.Lock()
	defer aps.lock.Unlock()

	if aps.someSafeStruct != nil {
		return *aps.someSafeStruct
	}

	return someStruct{}
}

const (
	process1Field = "process1"
	process2Field = "process2"
	process3Field = "process3"
	process4Field = "process4"
	process5Field = "process5"
)

func (aps *asyncProcessStruct) updateByField(someStruct someStruct, fields ...string) {
	aps.lock.Lock()
	defer aps.lock.Unlock()

	for _, field := range fields {
		switch field {
		case process1Field:
			aps.someSafeStruct.process1 = someStruct.process1
		case process2Field:
			aps.someSafeStruct.process2 = someStruct.process2
		case process3Field:
			aps.someSafeStruct.process3 = someStruct.process3
		case process4Field:
			aps.someSafeStruct.process4 = someStruct.process4
		case process5Field:
			aps.someSafeStruct.process5 = someStruct.process5
		}
	}
}

const (
	thread1 = iota
	thread2
	thread3
	thread4
	thread5
)

var (
	defaultTestThread = []int{thread1, thread2, thread3, thread4, thread5}
)

func buildTestThreadConfig(cfg map[int]func(ctx context.Context) chan error) map[int]*Thread {
	threads := make(map[int]*Thread, len(defaultTestThread))
	for i := range defaultTestThread {
		threads[i] = &Thread{
			Channel: make(chan error),
		}
	}

	threads[thread1].DependsOn = []*Thread{}
	threads[thread2].DependsOn = []*Thread{}
	threads[thread3].DependsOn = []*Thread{threads[thread1]}
	threads[thread4].DependsOn = []*Thread{threads[thread1], threads[thread2], threads[thread3]}
	threads[thread5].DependsOn = []*Thread{threads[thread1], threads[thread2], threads[thread3], threads[thread4]}

	threads[thread1].Action = cfg[thread1]
	threads[thread2].Action = cfg[thread2]
	threads[thread3].Action = cfg[thread3]
	threads[thread4].Action = cfg[thread4]
	threads[thread5].Action = cfg[thread5]

	return threads
}

func thread1Action(asyncStruct *asyncProcessStruct, returnErr bool, waitDelay *time.Duration) func(ctx context.Context) chan error {
	return func(ctx context.Context) chan error {
		return lo.Async(func() error {

			if waitDelay != nil {
				time.Sleep(*waitDelay)
			}

			if returnErr {
				return fmt.Errorf("err from thread 1")
			}

			localCopy := asyncStruct.getLocalCopy()

			localCopy.process1 = true

			asyncStruct.updateByField(localCopy, process1Field)

			return nil
		})
	}
}

func thread2Action(asyncStruct *asyncProcessStruct, returnErr bool, waitDelay *time.Duration) func(ctx context.Context) chan error {
	return func(ctx context.Context) chan error {
		return lo.Async(func() error {

			if waitDelay != nil {
				time.Sleep(*waitDelay)
			}

			if returnErr {
				return fmt.Errorf("err from thread 2")
			}

			localCopy := asyncStruct.getLocalCopy()

			localCopy.process2 = true

			asyncStruct.updateByField(localCopy, process2Field)

			return nil
		})
	}
}

func thread3Action(asyncStruct *asyncProcessStruct, returnErr bool, waitDelay *time.Duration) func(ctx context.Context) chan error {
	return func(ctx context.Context) chan error {
		return lo.Async(func() error {

			if waitDelay != nil {
				time.Sleep(*waitDelay)
			}

			if returnErr {
				return fmt.Errorf("err from thread 3")
			}

			localCopy := asyncStruct.getLocalCopy()

			localCopy.process3 = true

			asyncStruct.updateByField(localCopy, process3Field)

			return nil
		})
	}
}

func thread4Action(asyncStruct *asyncProcessStruct, returnErr bool, waitDelay *time.Duration) func(ctx context.Context) chan error {
	return func(ctx context.Context) chan error {
		return lo.Async(func() error {

			if waitDelay != nil {
				time.Sleep(*waitDelay)
			}

			if returnErr {
				return fmt.Errorf("err from thread 4")
			}

			localCopy := asyncStruct.getLocalCopy()

			localCopy.process4 = true

			asyncStruct.updateByField(localCopy, process4Field)

			return nil
		})
	}
}

func thread5Action(asyncStruct *asyncProcessStruct, returnErr bool, waitDelay *time.Duration) func(ctx context.Context) chan error {
	return func(ctx context.Context) chan error {
		return lo.Async(func() error {

			if waitDelay != nil {
				time.Sleep(*waitDelay)
			}

			if returnErr {
				return fmt.Errorf("err from thread 5")
			}

			localCopy := asyncStruct.getLocalCopy()

			localCopy.process5 = true

			asyncStruct.updateByField(localCopy, process5Field)

			return nil
		})
	}
}

func TestAsyncRunner(t *testing.T) {
	t.Run("Success all threads", func(t *testing.T) {
		t.Parallel()

		asp := &asyncProcessStruct{
			lock:           &sync.Mutex{},
			someSafeStruct: &someStruct{},
		}

		ctx := context.Background()
		masterChannel := make(chan error)

		threadCfg := map[int]func(ctx context.Context) chan error{
			thread1: thread1Action(asp, false, nil),
			thread2: thread2Action(asp, false, nil),
			thread3: thread3Action(asp, false, nil),
			thread4: thread4Action(asp, false, nil),
			thread5: thread5Action(asp, false, nil),
		}

		go Run(ctx, buildTestThreadConfig(threadCfg), masterChannel)

		var err error
		select {
		case err = <-masterChannel:
		case <-ctx.Done():
		}

		assert.Equal(t, true, asp.someSafeStruct.process1)
		assert.Equal(t, true, asp.someSafeStruct.process2)
		assert.Equal(t, true, asp.someSafeStruct.process3)
		assert.Equal(t, true, asp.someSafeStruct.process4)
		assert.Equal(t, true, asp.someSafeStruct.process5)
		assert.ErrorIs(t, nil, err)
	})

	t.Run("Success 1-3 threads. 4-5 failed", func(t *testing.T) {
		t.Parallel()

		asp := &asyncProcessStruct{
			lock:           &sync.Mutex{},
			someSafeStruct: &someStruct{},
		}

		ctx := context.Background()
		masterChannel := make(chan error)

		threadCfg := map[int]func(ctx context.Context) chan error{
			thread1: thread1Action(asp, false, nil),
			thread2: thread2Action(asp, false, nil),
			thread3: thread3Action(asp, false, nil),
			thread4: thread4Action(asp, true, nil),
			thread5: thread5Action(asp, false, nil),
		}

		go Run(ctx, buildTestThreadConfig(threadCfg), masterChannel)

		var err error
		select {
		case err = <-masterChannel:
		case <-ctx.Done():
		}

		assert.Equal(t, true, asp.someSafeStruct.process1)
		assert.Equal(t, true, asp.someSafeStruct.process2)
		assert.Equal(t, true, asp.someSafeStruct.process3)
		assert.Equal(t, false, asp.someSafeStruct.process4)
		assert.Equal(t, false, asp.someSafeStruct.process5)
		assert.Error(t, err, "err from thread 4")
	})

	t.Run("Success 2 threads. 1,3-5 failed", func(t *testing.T) {
		t.Parallel()

		asp := &asyncProcessStruct{
			lock:           &sync.Mutex{},
			someSafeStruct: &someStruct{},
		}

		ctx := context.Background()
		masterChannel := make(chan error)

		threadCfg := map[int]func(ctx context.Context) chan error{
			thread1: thread1Action(asp, true, nil),
			thread2: thread2Action(asp, false, nil),
			thread3: thread3Action(asp, true, nil),
			thread4: thread4Action(asp, true, nil),
			thread5: thread5Action(asp, true, nil),
		}

		go Run(ctx, buildTestThreadConfig(threadCfg), masterChannel)

		var err error
		select {
		case err = <-masterChannel:
		case <-ctx.Done():
		}

		assert.Equal(t, false, asp.someSafeStruct.process1)
		assert.Equal(t, true, asp.someSafeStruct.process2)
		assert.Equal(t, false, asp.someSafeStruct.process3)
		assert.Equal(t, false, asp.someSafeStruct.process4)
		assert.Equal(t, false, asp.someSafeStruct.process5)
		assert.Error(t, err, "err from thread 1")
	})

	t.Run("Thread start and return err. Wait other started treads", func(t *testing.T) {
		t.Parallel()

		asp := &asyncProcessStruct{
			lock:           &sync.Mutex{},
			someSafeStruct: &someStruct{},
		}

		ctx := context.Background()
		masterChannel := make(chan error)

		timeSecond := 100 * time.Millisecond

		threadCfg := map[int]func(ctx context.Context) chan error{
			thread1: thread1Action(asp, false, &timeSecond),
			thread2: thread2Action(asp, true, nil),
			thread3: thread3Action(asp, true, nil),
			thread4: thread4Action(asp, true, nil),
			thread5: thread5Action(asp, true, nil),
		}

		go Run(ctx, buildTestThreadConfig(threadCfg), masterChannel)

		var err error
		select {
		case err = <-masterChannel:
		case <-ctx.Done():
		}

		assert.Equal(t, true, asp.someSafeStruct.process1)
		assert.Equal(t, false, asp.someSafeStruct.process2)
		assert.Equal(t, false, asp.someSafeStruct.process3)
		assert.Equal(t, false, asp.someSafeStruct.process4)
		assert.Equal(t, false, asp.someSafeStruct.process5)
		assert.Error(t, err, "err from thread 2")
	})
}
