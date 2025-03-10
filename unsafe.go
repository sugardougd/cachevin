package cachevin

import (
	"container/heap"
	"sync/atomic"
	"time"
)

func NewUnsafe(capacity int, expire time.Duration, priorityFunc PriorityFunc) Cache {
	if capacity < 1 {
		log.Warnf("capacity must greater than zero.reset capacity 1")
		capacity = 1
	}
	cache := unsafeCache{
		capacity: capacity,
		expire:   expire,
		expireT:  defaultExpireT,
		cache:    make(map[Key]*entity),
		priorityQueue: &PriorityQueue{
			queue: make([]*entity, 0),
			fun:   priorityFunc,
		},
		abort: make(chan struct{}),
	}
	cache.state.Store(stateRunning)
	go cache.run()
	log.Infof("NewUnsafeCache capacity=%d, expire=%v", capacity, expire)
	return &cache
}

// unsafeCache 并发不安全的缓存
type unsafeCache struct {
	capacity      int
	expire        time.Duration   // 缓存过期时间
	expireT       time.Duration   // 每100ms执行一次，检查key是否过期
	cache         map[Key]*entity // 存放缓存实体的map
	priorityQueue *PriorityQueue  // 优先级队列，按优先级淘汰
	state         atomic.Int32    // 状态
	abort         chan struct{}
}

func (cache *unsafeCache) run() {
	ticker := time.NewTicker(cache.expireT)
	for cache.isRunning() {
		select {
		case <-cache.abort:
			// 缓存 Close 了
			log.Infof("Exit run")
		case <-ticker.C:
			// 检查key是否过期
			cache.checkAndExpire()
		}
	}
	ticker.Stop()
	cache.clear()
}

func (cache *unsafeCache) checkAndExpire() {
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

func (cache *unsafeCache) set(key Key, value Value) error {
	if _, found := cache.cache[key]; !found {
		// 如果超过 capacity，淘汰缓存数据
		cache.eviction()
	}
	e := cache.newEntity(key, value)
	cache.setEntity(key, e)
	return nil
}

func (cache *unsafeCache) get(key Key) (Value, error) {
	e := cache.cache[key]
	if e == nil {
		return nil, NotFound
	} else if e.isExpire() {
		// 缓存已过期，删除
		log.Infof("%s is expire", e.key)
		cache.removeEntity(key, e)
		return nil, NotFound
	} else {
		e.lastAccess = time.Now()
		e.accessCount++
		// update PriorityQueue
		cache.priorityQueue.update(e)
		return e.value, nil
	}
}

func (cache *unsafeCache) getWithLoad(key Key, fun ValueFunc) (Value, error) {
	e := cache.cache[key]
	if e == nil {
		// 如果超过 capacity，淘汰缓存数据
		cache.eviction()
		value, err := fun(key)
		if err != nil {
			return nil, err
		}
		// 缓存中没有记录，计算结果并存入缓存后返回
		e = cache.newEntity(key, value)
		cache.setEntity(key, e)
		return value, nil
	}
	if e.isExpire() {
		// 缓存已过期，删除后重新添加
		log.Infof("%s is expire", e.key)
		cache.removeEntity(key, e)
		value, err := fun(key)
		if err != nil {
			return nil, err
		}
		e = cache.newEntity(key, value)
		cache.setEntity(key, e)
		return value, nil
	}
	e.lastAccess = time.Now()
	e.accessCount++
	// update PriorityQueue
	cache.priorityQueue.update(e)
	return e.value, nil
}

func (cache *unsafeCache) remove(key Key) error {
	e := cache.cache[key]
	if e != nil {
		cache.removeEntity(key, e)
	}
	return nil
}

func (cache *unsafeCache) clear() error {
	cache.cache = make(map[Key]*entity, 0)
	cache.priorityQueue.clear()
	log.Infof("Clear cache")
	return nil
}

func (cache *unsafeCache) newEntity(k Key, v Value) *entity {
	return &entity{
		key:         k,
		value:       v,
		cacheTime:   time.Now(),
		accessCount: 1,
		lastAccess:  time.Now(),
		expire:      cache.expire,
		expireTime:  time.Now().Add(cache.expire),
	}
}

func (cache *unsafeCache) setEntity(key Key, e *entity) {
	cache.cache[key] = e
	heap.Push(cache.priorityQueue, e)
	log.Infof("set entity. key: %s", e.key)
}

func (cache *unsafeCache) removeEntity(key Key, e *entity) {
	delete(cache.cache, key)
	heap.Remove(cache.priorityQueue, e.index)
	log.Infof("remove entity. key: %s", e.key)
}

func (cache *unsafeCache) eviction() {
	for cache.Len()+1 > cache.capacity && cache.priorityQueue.Len() > 0 {
		e := heap.Pop(cache.priorityQueue).(*entity)
		if e != nil {
			delete(cache.cache, e.key)
			log.Infof("eviction entity. key: %s", e.key)
		}
	}
}

func (cache *unsafeCache) isRunning() bool {
	return stateRunning == cache.state.Load()
}

func (cache *unsafeCache) Len() int {
	return len(cache.cache)
}

func (cache *unsafeCache) Set(key Key, value Value) error {
	if !cache.isRunning() {
		return NotAvailable
	}
	return cache.set(key, value)
}

func (cache *unsafeCache) SetWithTimeout(key Key, value Value, timeout time.Duration) error {
	return NotSupport
}

func (cache *unsafeCache) Has(key Key) (bool, error) {
	if !cache.isRunning() {
		return false, NotAvailable
	}
	_, err := cache.get(key)
	return err == nil, err
}

func (cache *unsafeCache) HasWithTimeout(key Key, timeout time.Duration) (bool, error) {
	return false, NotSupport
}

func (cache *unsafeCache) Get(key Key) (Value, error) {
	if !cache.isRunning() {
		return nil, NotAvailable
	}
	return cache.get(key)
}

func (cache *unsafeCache) GetWithTimeout(key Key, timeout time.Duration) (Value, error) {
	return nil, NotSupport
}

func (cache *unsafeCache) GetLoad(key Key, fun ValueFunc) (Value, error) {
	if !cache.isRunning() {
		return nil, NotAvailable
	}
	return cache.getWithLoad(key, fun)
}

func (cache *unsafeCache) GetLoadWithTimeout(key Key, fun ValueFunc, timeout time.Duration) (Value, error) {
	return nil, NotSupport
}

func (cache *unsafeCache) Remove(key Key) error {
	if !cache.isRunning() {
		return NotAvailable
	}
	return cache.remove(key)
}

func (cache *unsafeCache) RemoveWithTimeout(key Key, timeout time.Duration) error {
	return NotSupport
}

func (cache *unsafeCache) Clear() error {
	if !cache.isRunning() {
		return NotAvailable
	}
	return cache.clear()
}

func (cache *unsafeCache) ClearWithTimeout(timeout time.Duration) error {
	return NotSupport
}

func (cache *unsafeCache) Close() {
	close(cache.abort)
	cache.state.Store(stateTerminate)
	log.Infof("set state: %v", stateTerminate)
}
