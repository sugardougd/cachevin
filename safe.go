package cachevin

import (
	"container/heap"
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	stateRunning = iota
	stateTerminate
)
const (
	operationSet = iota
	operationHas
	operationGet
	operationGetWithLoad
	operationRemove
	operationRemoveErr
	operationClear
)

// NewSafe 创建一个并发安全的缓存
// capacity 为缓存大小，超出大小后再添加缓存前，会按淘汰算法删除部分缓存
// expire 缓存过期时间，到期后自动清理缓存
// priorityFunc 缓存淘汰策略
func NewSafe(capacity int, expire time.Duration, priorityFunc PriorityFunc) Cache {
	if capacity < 1 {
		log.Warnf("capacity must greater than zero.reset capacity 1")
		capacity = 1
	}
	cache := safeCache{
		capacity:  capacity,
		expire:    expire,
		expireT:   defaultExpireT,
		timeout:   defaultTimeout,
		cache:     make(map[Key]*entity),
		operation: make(chan *operation),
		priorityQueue: &PriorityQueue{
			queue: make([]*entity, 0),
			fun:   priorityFunc,
		},
	}
	cache.abort, cache.cancel = context.WithCancel(context.Background())
	cache.state.Store(stateRunning)
	go cache.run()
	log.Infof("NewSafeCache capacity=%d, expire=%v", capacity, expire)
	return &cache
}

// safeCache 非阻塞并发安全的缓存
// 通过 channel 实现 goroutine 之间的同步
type safeCache struct {
	capacity      int
	expire        time.Duration      // 缓存过期时间
	timeout       time.Duration      // 超时时间
	expireT       time.Duration      // 每100ms执行一次，检查key是否过期
	cache         map[Key]*entity    // 存放缓存实体的map
	priorityQueue *PriorityQueue     // 优先级队列，按优先级淘汰
	operation     chan *operation    // 操作缓存 chan，所有操作在主 goroutine 执行，保证并发安全
	state         atomic.Int32       // 状态
	abort         context.Context    //
	cancel        context.CancelFunc //
}

type operation struct {
	action int
	key    Key
	value  Value
	fun    ValueFunc
	resp   chan response
}

type response struct {
	result Value
	err    error
}

func (cache *safeCache) run() {
	ticker := time.NewTicker(cache.expireT)
	for cache.isRunning() {
		select {
		case <-cache.abort.Done():
			// 缓存 Close 了
			log.Infof("Exit run")
		case <-ticker.C:
			// 检查key是否过期
			cache.checkAndExpire()
		case opera := <-cache.operation:
			switch opera.action {
			case operationSet:
				// 添加缓存key-value
				err := cache.set(opera.key, opera.value)
				opera.resp <- response{err: err}
				break
			case operationHas, operationGet:
				// 是否存在指定Key的缓存,通过 Key 获取缓存的 Value
				cache.get(opera)
				break
			case operationGetWithLoad:
				// 通过 Key 获取缓存的 Value
				cache.getWithLoad(opera)
				break
			case operationRemove, operationRemoveErr:
				// 通过 Key 删除缓存数据
				err := cache.remove(opera.key)
				opera.resp <- response{err: err}
				break
			case operationClear:
				// 清除缓存
				err := cache.clear()
				opera.resp <- response{err: err}
				break
			default:
				log.Warnf("un-support action: %d", opera.action)
				opera.resp <- response{err: fmt.Errorf("un-support action: %d", opera.action)}
				break
			}
		}
	}
	ticker.Stop()
	cache.clear()
}

func (cache *safeCache) checkAndExpire() {
	// 每100ms执行一次
	// 执行时，随机从缓存总去20个元素，检查是否过期，如果发现过期了就直接删除
	// 删除完检查过期的key超过25%，重复上一步
	// 执行时间超过25ms，退出，防止主goroutine阻塞
	timer := time.NewTimer(time.Millisecond * 25)
	done := false
	for !done {
		select {
		case <-timer.C:
			log.Warnf("check expire cost 25ms")
			done = true
		default:
			expireK := make([]string, 0)
			index := 0
			for k, v := range cache.cache {
				if index >= 20 {
					break
				}
				if v.isExpire() {
					expireK = append(expireK, k)
				}
				index++
			}
			for _, k := range expireK {
				log.Infof("%s is expire", k)
				cache.remove(k)
			}
			done = len(expireK) <= 5
		}
	}
	timer.Stop()
}

func (cache *safeCache) set(key Key, value Value) error {
	if _, found := cache.cache[key]; !found {
		// 如果超过 capacity，淘汰缓存数据
		cache.eviction()
	}
	e := cache.newEntity(key, value)
	cache.setEntity(key, e)
	close(e.ready)
	return nil
}

func (cache *safeCache) get(opera *operation) {
	e := cache.cache[opera.key]
	if e == nil {
		opera.resp <- response{err: NotFound}
	} else if e.isExpire() {
		// 缓存已过期，删除
		log.Infof("%s is expire", e.key)
		cache.removeEntity(opera.key, e)
		opera.resp <- response{err: NotFound}
	} else {
		e.lastAccess = time.Now()
		e.accessCount++
		// update PriorityQueue
		cache.priorityQueue.update(e)
		go e.deliver(opera.resp)
	}
}

func (cache *safeCache) getWithLoad(opera *operation) {
	e := cache.cache[opera.key]
	if e == nil {
		// 如果超过 capacity，淘汰缓存数据
		cache.eviction()
		// 缓存中没有记录，计算结果并存入缓存后返回
		e = cache.newEntity(opera.key, opera.value)
		cache.setEntity(opera.key, e)
		go e.call(cache, opera.key, opera.fun)
	} else if e.isExpire() {
		// 缓存已过期，删除后重新添加
		log.Infof("%s is expire", e.key)
		cache.removeEntity(opera.key, e)
		e = cache.newEntity(opera.key, opera.value)
		cache.setEntity(opera.key, e)
		go e.call(cache, opera.key, opera.fun)
	} else {
		e.lastAccess = time.Now()
		e.accessCount++
		// update PriorityQueue
		cache.priorityQueue.update(e)
	}
	go e.deliver(opera.resp)
}

func (cache *safeCache) remove(key Key) error {
	e := cache.cache[key]
	if e != nil {
		cache.removeEntity(key, e)
	}
	return nil
}

func (cache *safeCache) clear() error {
	cache.cache = make(map[Key]*entity, 0)
	cache.priorityQueue.clear()
	log.Infof("Clear cache")
	return nil
}

func (cache *safeCache) newEntity(k Key, v Value) *entity {
	return &entity{
		key:         k,
		value:       v,
		cacheTime:   time.Now(),
		accessCount: 1,
		lastAccess:  time.Now(),
		expire:      cache.expire,
		expireTime:  time.Now().Add(cache.expire),
		ready:       make(chan struct{}),
	}
}

func (cache *safeCache) setEntity(key Key, e *entity) {
	cache.cache[key] = e
	heap.Push(cache.priorityQueue, e)
	log.Infof("set entity. key: %s", e.key)
}

func (cache *safeCache) removeEntity(key Key, e *entity) {
	delete(cache.cache, key)
	heap.Remove(cache.priorityQueue, e.index)
	log.Infof("remove entity. key: %s", e.key)
}

func (cache *safeCache) eviction() {
	for cache.Len()+1 > cache.capacity && cache.priorityQueue.Len() > 0 {
		e := heap.Pop(cache.priorityQueue).(*entity)
		if e != nil {
			delete(cache.cache, e.key)
			log.Infof("eviction entity. key: %s", e.key)
		}
	}
}

func (cache *safeCache) isRunning() bool {
	return stateRunning == cache.state.Load()
}

func (cache *safeCache) Len() int {
	return len(cache.cache)
}

func (cache *safeCache) Set(key Key, value Value) error {
	return cache.SetWithTimeout(key, value, cache.timeout)
}

func (cache *safeCache) SetWithTimeout(key Key, value Value, timeout time.Duration) error {
	if !cache.isRunning() {
		return NotAvailable
	}
	// 超时控制
	timer := time.NewTimer(timeout)
	resp := make(chan response)
	cache.operation <- &operation{
		action: operationSet,
		key:    key,
		value:  value,
		resp:   resp,
	}
	select {
	case <-timer.C:
		return fmt.Errorf("timeout")
	case r := <-resp:
		timer.Stop()
		return r.err
	}
}

func (cache *safeCache) Has(key Key) (bool, error) {
	return cache.HasWithTimeout(key, cache.timeout)
}

func (cache *safeCache) HasWithTimeout(key Key, timeout time.Duration) (bool, error) {
	if !cache.isRunning() {
		return false, NotAvailable
	}
	// 超时控制
	timer := time.NewTimer(timeout)
	resp := make(chan response)
	cache.operation <- &operation{
		action: operationHas,
		key:    key,
		resp:   resp,
	}
	select {
	case <-timer.C:
		return false, fmt.Errorf("timeout")
	case r := <-resp:
		timer.Stop()
		return r.err == nil, r.err
	}
}

func (cache *safeCache) Get(key Key) (Value, error) {
	return cache.GetWithTimeout(key, cache.timeout)
}

func (cache *safeCache) GetWithTimeout(key Key, timeout time.Duration) (Value, error) {
	if !cache.isRunning() {
		return nil, NotAvailable
	}
	// 超时控制
	timer := time.NewTimer(timeout)
	resp := make(chan response)
	cache.operation <- &operation{
		action: operationGet,
		key:    key,
		resp:   resp,
	}
	select {
	case <-timer.C:
		return nil, fmt.Errorf("timeout")
	case r := <-resp:
		timer.Stop()
		return r.result, r.err
	}
}

func (cache *safeCache) GetLoad(key Key, fun ValueFunc) (Value, error) {
	return cache.GetLoadWithTimeout(key, fun, cache.timeout)
}

func (cache *safeCache) GetLoadWithTimeout(key Key, fun ValueFunc, timeout time.Duration) (Value, error) {
	if !cache.isRunning() {
		return nil, NotAvailable
	}
	// 超时控制
	timer := time.NewTimer(timeout)
	resp := make(chan response)
	cache.operation <- &operation{
		action: operationGetWithLoad,
		key:    key,
		fun:    fun,
		resp:   resp,
	}
	select {
	case <-timer.C:
		return nil, fmt.Errorf("timeout")
	case r := <-resp:
		timer.Stop()
		return r.result, r.err
	}
}

func (cache *safeCache) Remove(key Key) error {
	return cache.RemoveWithTimeout(key, cache.timeout)
}

func (cache *safeCache) RemoveWithTimeout(key Key, timeout time.Duration) error {
	if !cache.isRunning() {
		return NotAvailable
	}
	// 超时控制
	timer := time.NewTimer(timeout)
	resp := make(chan response)
	cache.operation <- &operation{
		action: operationRemove,
		key:    key,
		resp:   resp,
	}
	select {
	case <-timer.C:
		return fmt.Errorf("timeout")
	case r := <-resp:
		timer.Stop()
		return r.err
	}
}

func (cache *safeCache) Clear() error {
	return cache.ClearWithTimeout(cache.timeout)
}

func (cache *safeCache) ClearWithTimeout(timeout time.Duration) error {
	if !cache.isRunning() {
		return NotAvailable
	}
	// 超时控制
	timer := time.NewTimer(timeout)
	resp := make(chan response)
	cache.operation <- &operation{
		action: operationClear,
		resp:   resp,
	}
	select {
	case <-timer.C:
		return fmt.Errorf("timeout")
	case r := <-resp:
		timer.Stop()
		return r.err
	}
}

func (cache *safeCache) Close() {
	cache.state.Store(stateTerminate)
	log.Infof("set state: %v", stateTerminate)
	cache.cancel()
}
