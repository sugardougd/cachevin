package cachevin

import (
	"container/heap"
	"errors"
	"github.com/sugardougd/logvin"
	"time"
)

const defaultTimeout = time.Minute
const defaultExpireT = time.Millisecond * 100

var log = logvin.New("cache")

var NotFound = errors.New("un-found")
var NotAvailable = errors.New("un-available")
var NotSupport = errors.New("un-support")

type Key = string
type Value = interface{}
type ValueFunc func(Key) (Value, error)

type Cache interface {
	// Len 获取缓存大小
	Len() int

	// Set 添加缓存key-value,超时返回error
	Set(key Key, value Value) error
	// SetWithTimeout 添加缓存key-value,可指定超时时间
	SetWithTimeout(key Key, value Value, timeout time.Duration) error

	// Has 是否包含指定Key的缓存，超时返回 error
	Has(key Key) (bool, error)
	// HasWithTimeout 是否包含指定Key的缓存，超时返回 error,可指定超时时间
	HasWithTimeout(key Key, timeout time.Duration) (bool, error)

	// Get 通过 Key 获取缓存的 Value，缓存不存在返回 error，超时返回 error
	Get(key Key) (Value, error)
	// GetWithTimeout 通过 Key 获取缓存的 Value，缓存不存在返回 error，超时返回 error,可指定超时时间
	GetWithTimeout(key Key, timeout time.Duration) (Value, error)

	// GetLoad 通过 Key 获取缓存的 Value，缓存不存在时，计算新的value，并加入缓存中，超时返回 error
	GetLoad(key Key, fun ValueFunc) (Value, error)
	// GetLoadWithTimeout 通过 Key 获取缓存的 Value，缓存不存在时，计算新的value，并加入缓存中，超时返回 error,可指定超时时间
	GetLoadWithTimeout(key Key, fun ValueFunc, timeout time.Duration) (Value, error)

	// Remove 通过 Key 删除缓存数据,超时返回 error
	Remove(key Key) error
	// RemoveWithTimeout 通过 Key 删除缓存数据,超时返回 error,可指定超时时间
	RemoveWithTimeout(key Key, timeout time.Duration) error

	// Clear 清除缓存,超时返回 error
	Clear() error
	// ClearWithTimeout 清除缓存,超时返回 error,可指定超时时间
	ClearWithTimeout(timeout time.Duration) error

	// Close 停止缓存组件,停止后，再操作缓存返回 error
	Close()
}

type entity struct {
	key         Key
	value       Value
	cacheTime   time.Time
	accessCount int
	lastAccess  time.Time
	expire      time.Duration
	expireTime  time.Time
	index       int // The index of the item in the heap.
	version     int
	err         error
	ready       chan struct{}
}

func (e *entity) isExpire() bool {
	return e.expire > 0 && time.Now().After(e.expireTime)
}

func (e *entity) call(cache *safeCache, key Key, fun ValueFunc) {
	// Broadcast the ready condition.
	defer close(e.ready)
	// Evaluate the function.
	e.value, e.err = fun(key)
	// Callback
	if e.err != nil {
		resp := make(chan response)
		cache.operation <- &operation{
			action: operationRemoveErr,
			key:    key,
			resp:   resp,
		}
		// Wait for remove
		<-resp
	}
}

func (e *entity) deliver(value chan<- response) {
	// Wait for the ready condition.
	<-e.ready
	// Send the result to the client.
	value <- response{result: e.value, err: e.err}
}

// PriorityFunc 权重计算函数，优先淘汰权重最小的缓存，e1权重比e2低时，返回true
type PriorityFunc func(e1, e2 *entity) bool

// FIFOPriorityFunc (First in First out)先进先出
func FIFOPriorityFunc(e1, e2 *entity) bool {
	return e1.cacheTime.Before(e2.cacheTime)
}

// LRUPriorityFunc (The Least Recently Used) 最近最久未使用
func LRUPriorityFunc(e1, e2 *entity) bool {
	return e1.lastAccess.Before(e2.lastAccess)
}

// LFUPriorityFunc （The Least Frequently Used）最近不多使用
func LFUPriorityFunc(e1, e2 *entity) bool {
	return e1.accessCount < e2.accessCount
}

type PriorityQueue struct {
	queue []*entity
	fun   PriorityFunc
}

// Push x is *entity
func (p *PriorityQueue) Push(x any) {
	n := len(p.queue)
	e := x.(*entity)
	e.index = n
	p.queue = append(p.queue, e)
}

// Pop removes and returns the minimum element *entity
func (p *PriorityQueue) Pop() any {
	if p.Len() == 0 {
		return nil
	}
	n := len(p.queue)
	e := p.queue[n-1]
	p.queue[n-1] = nil
	e.index = -1
	p.queue = p.queue[:n-1]
	return e
}

// Peek returns the minimum element *entity
func (p *PriorityQueue) Peek() any {
	if p.Len() == 0 {
		return nil
	}
	return p.queue[0]
}

func (p *PriorityQueue) Len() int {
	return len(p.queue)
}

func (p *PriorityQueue) Less(i, j int) bool {
	return p.fun(p.queue[i], p.queue[j])
}

func (p *PriorityQueue) Swap(i, j int) {
	p.queue[i], p.queue[j] = p.queue[j], p.queue[i]
	p.queue[i].index = i
	p.queue[j].index = j
}

// update modifies the priorityQueue and value of an Item in the queue.
func (p *PriorityQueue) update(entity *entity) {
	heap.Fix(p, entity.index)
}

func (p *PriorityQueue) clear() {
	p.queue = make([]*entity, 0)
}
