package cachevin

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func Test_safeFIFOCache_Has(t *testing.T) {
	// Test Has
	t.Run("Test_safeFIFOCache_Has", func(t *testing.T) {
		cache := NewSafe(3, 0, FIFOPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != len(items) {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items))
		}
		for _, i := range items {
			if has, _ := cache.Has(i); !has {
				t.Errorf("cache not exist '%s',but want", i)
			}
		}
	})

	// Test HasWithTimeout
	t.Run("Test_safeFIFOCache_HasWithTimeout", func(t *testing.T) {
		cache := NewSafe(2, 0, FIFOPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c", "d"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != 2 {
			t.Errorf("cache len is %d want %d", cache.Len(), 2)
		}
		for _, i := range items[:2] {
			if has, _ := cache.HasWithTimeout(i, time.Second); has {
				t.Errorf("cache exist '%s',but not want", i)
			}
		}
		for _, i := range items[2:] {
			if has, _ := cache.Has(i); !has {
				t.Errorf("cache not exist '%s',but want", i)
			}
		}
	})
}

func Test_safeFIFOCache_Get(t *testing.T) {
	// Test Get
	t.Run("Test_safeFIFOCache_Get", func(t *testing.T) {
		cache := NewSafe(3, 0, FIFOPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != len(items) {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items))
		}

		for _, i := range items {
			if v, err := cache.Get(i); err == nil {
				log.Infof("Get() %v=%v", i, v)
			} else {
				t.Errorf("Get() %v error = %v", i, err)
			}
		}

	})

	// Test GetWithTimeout
	t.Run("Test_safeFIFOCache_GetWithTimeout", func(t *testing.T) {
		cache := NewSafe(2, 0, FIFOPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != 2 {
			t.Errorf("cache len is %d want %d", cache.Len(), 2)
		}

		for _, i := range items {
			if v, err := cache.GetWithTimeout(i, time.Second); err == nil || err == NotFound {
				log.Infof("GetWithTimeout() %v=%v", i, v)
			} else {
				t.Errorf("GetWithTimeout() %v error = %v", i, err)
			}
		}
	})
}

func Test_safeFIFOCache_GetLoad(t *testing.T) {
	// Test GetLoad
	t.Run("Test_safeFIFOCache_GetLoad", func(t *testing.T) {
		cache := NewSafe(3, 0, FIFOPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != len(items) {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items))
		}

		items = append(items, "e", "f")
		for _, i := range items {
			if v, err := cache.GetLoad(i, func(k Key) (Value, error) {
				return strings.ToUpper(k), nil
			}); err == nil || err == NotFound {
				log.Infof("GetLoad() %v=%v", i, v)
			} else {
				t.Errorf("GetLoad() %v error = %v", i, err)
			}
		}
		if cache.Len() != 3 {
			t.Errorf("cache len is %d want %d", cache.Len(), 3)
		}
	})

	// Test GetLoadWithTimeout
	t.Run("Test_safeFIFOCache_GetWithTimeout", func(t *testing.T) {
		cache := NewSafe(3, 0, FIFOPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != len(items) {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items))
		}

		items = append(items, "e", "f")
		for _, i := range items {
			if v, err := cache.GetLoadWithTimeout(i, func(k Key) (Value, error) {
				return strings.ToUpper(k), nil
			}, time.Second); err == nil || err == NotFound {
				log.Infof("GetLoadWithTimeout() %v=%v", i, v)
			} else {
				t.Errorf("GetLoadWithTimeout() %v error = %v", i, err)
			}
		}
		if cache.Len() != 3 {
			t.Errorf("cache len is %d want %d", cache.Len(), 3)
		}
	})
}

func Test_safeFIFOCache_GetLoadErr(t *testing.T) {
	// Test GetLoad
	t.Run("Test_safeFIFOCache_GetLoad", func(t *testing.T) {
		cache := NewSafe(3, 0, FIFOPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c"}
		for _, i := range items {
			if _, err := cache.GetLoad(i, func(k Key) (Value, error) {
				return strings.ToUpper(k), errors.New("test")
			}); err == nil || err == NotFound {
				t.Errorf("GetLoad(%v) success,but want error", i)
			}
		}
		if cache.Len() != 0 {
			t.Errorf("cache len is %d want %d", cache.Len(), 0)
		}
	})
}

func Test_safeFIFOCache_Remove(t *testing.T) {
	// Test Remove
	t.Run("Test_safeFIFOCache_Remove", func(t *testing.T) {
		cache := NewSafe(3, 0, FIFOPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != len(items) {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items))
		}

		if err := cache.Remove("a"); err != nil {
			t.Errorf("Remove %v error %v", "a", err)
		}
		if cache.Len() != len(items)-1 {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items)-1)
		}
	})

	// Test Remove
	t.Run("Test_safeFIFOCache_RemoveWithTimeout", func(t *testing.T) {
		cache := NewSafe(3, 0, FIFOPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != len(items) {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items))
		}

		if err := cache.RemoveWithTimeout("a", time.Second); err != nil {
			t.Errorf("RemoveWithTimeout %v error %v", "a", err)
		}
		if cache.Len() != len(items)-1 {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items)-1)
		}
	})
}

func Test_safeFIFOCache_Clear(t *testing.T) {
	// Test Remove
	t.Run("Test_safeFIFOCache_Clear", func(t *testing.T) {
		cache := NewSafe(3, 0, FIFOPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != len(items) {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items))
		}

		if err := cache.Clear(); err != nil {
			t.Errorf("Clear error %v", err)
		}
		if cache.Len() != 0 {
			t.Errorf("cache len is %d want %d", cache.Len(), 0)
		}
	})

	// Test ClearWithTimeout
	t.Run("Test_safeFIFOCache_ClearWithTimeout", func(t *testing.T) {
		cache := NewSafe(3, 0, FIFOPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != len(items) {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items))
		}

		if err := cache.ClearWithTimeout(time.Second); err != nil {
			t.Errorf("ClearWithTimeout error %v", err)
		}
		if cache.Len() != 0 {
			t.Errorf("cache len is %d want %d", cache.Len(), 0)
		}
	})
}

func Test_safeFIFOCache_Close(t *testing.T) {
	// Test Remove
	t.Run("Test_safeFIFOCache_Close", func(t *testing.T) {
		cache := NewSafe(3, 0, FIFOPriorityFunc)
		items := []string{"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != len(items) {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items))
		}

		cache.Close()
		if cache.Clear() == nil {
			t.Errorf("cache is not closed")
		}
	})
}

func Test_safeLFUCache_Has(t *testing.T) {
	// Test Has
	t.Run("Test_safeLFUCache_Has", func(t *testing.T) {
		cache := NewSafe(3, 0, LFUPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c"}
		// {"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != len(items) {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items))
		}
		for _, i := range items {
			if has, _ := cache.Has(i); !has {
				t.Errorf("cache not exist '%s',but want", i)
			}
		}

		// improve a/b priorityQueue
		// {"c", "a", "b"}
		for _, i := range items[:2] {
			if has, _ := cache.Has(i); !has {
				t.Errorf("cache not exist '%s',but want", i)
			}
		}

		// improve a priorityQueue
		// {"c", "b", "a"}
		key := "a"
		for i := 0; i < 2; i++ {
			if has, _ := cache.Has(key); !has {
				t.Errorf("cache not exist '%s',but want", key)
			}
		}

		// add d
		// eviction c -> {"d", "b", "a"}
		key = "d"
		items = append(items, key)
		if v, err := cache.GetLoad(key, func(k Key) (Value, error) {
			return strings.ToUpper(k), nil
		}); err == nil || err == NotFound {
			log.Infof("GetLoad() %v=%v", key, v)
		} else {
			t.Errorf("GetLoad() %v error = %v", key, err)
		}
		if has, _ := cache.Has(key); !has {
			t.Errorf("cache not exist '%s',but want", key)
		}
		key = "c"
		if has, _ := cache.Has(key); has {
			t.Errorf("cache exist '%s',but not want", key)
		}

		// add e
		// eviction d -> {"e", "b", "a"}
		key = "e"
		items = append(items, key)
		if v, err := cache.GetLoad(key, func(k Key) (Value, error) {
			return strings.ToUpper(k), nil
		}); err == nil || err == NotFound {
			log.Infof("GetLoad() %v=%v", key, v)
		} else {
			t.Errorf("GetLoad() %v error = %v", key, err)
		}
		if has, _ := cache.Has(key); !has {
			t.Errorf("cache not exist '%s',but want", key)
		}
		key = "d"
		if has, _ := cache.Has(key); has {
			t.Errorf("cache exist '%s',but not want", key)
		}

		// improve e priorityQueue
		// {"b", "e", "a"}
		key = "e"
		for i := 0; i < 2; i++ {
			if has, _ := cache.Has(key); !has {
				t.Errorf("cache not exist '%s',but want", key)
			}
		}

		// add f
		// eviction b -> {"f", "e", "a"}
		key = "f"
		items = append(items, key)
		if v, err := cache.GetLoad(key, func(k Key) (Value, error) {
			return strings.ToUpper(k), nil
		}); err == nil || err == NotFound {
			log.Infof("GetLoad() %v=%v", key, v)
		} else {
			t.Errorf("GetLoad() %v error = %v", key, err)
		}
		if has, _ := cache.Has(key); !has {
			t.Errorf("cache not exist '%s',but want", key)
		}
		key = "b"
		if has, _ := cache.Has(key); has {
			t.Errorf("cache exist '%s',but not want", key)
		}

		// add g
		// eviction f -> {"g", "e", "a"}
		key = "g"
		items = append(items, key)
		if v, err := cache.GetLoad(key, func(k Key) (Value, error) {
			return strings.ToUpper(k), nil
		}); err == nil || err == NotFound {
			log.Infof("GetLoad() %v=%v", key, v)
		} else {
			t.Errorf("GetLoad() %v error = %v", key, err)
		}
		if has, _ := cache.Has(key); !has {
			t.Errorf("cache not exist '%s',but want", key)
		}
		key = "f"
		if has, _ := cache.Has(key); has {
			t.Errorf("cache exist '%s',but not want", key)
		}
	})
}

func Test_safeLRUCache_Has(t *testing.T) {
	// Test Has
	t.Run("Test_safeLRUCache_Has", func(t *testing.T) {
		cache := NewSafe(3, 0, LRUPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c"}
		// {"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != len(items) {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items))
		}
		for _, i := range items {
			if has, _ := cache.Has(i); !has {
				t.Errorf("cache not exist '%s',but want", i)
			}
		}

		// improve a/b priorityQueue
		// {"c", "a", "b"}
		for _, i := range items[:2] {
			if has, _ := cache.Has(i); !has {
				t.Errorf("cache not exist '%s',but want", i)
			}
		}

		// add d
		// {"a", "b", "d"}
		key := "d"
		items = append(items, key)
		if v, err := cache.GetLoad(key, func(k Key) (Value, error) {
			return strings.ToUpper(k), nil
		}); err == nil || err == NotFound {
			log.Infof("GetLoad() %v=%v", key, v)
		} else {
			t.Errorf("GetLoad() %v error = %v", key, err)
		}
		if has, _ := cache.Has(key); !has {
			t.Errorf("cache not exist '%s',but want", key)
		}
		key = "c"
		if has, _ := cache.Has(key); has {
			t.Errorf("cache exist '%s',but not want", key)
		}

		// add e
		// eviction a -> {"b", "d", "e"}
		key = "e"
		items = append(items, key)
		if v, err := cache.GetLoad(key, func(k Key) (Value, error) {
			return strings.ToUpper(k), nil
		}); err == nil || err == NotFound {
			log.Infof("GetLoad() %v=%v", key, v)
		} else {
			t.Errorf("GetLoad() %v error = %v", key, err)
		}
		if has, _ := cache.Has(key); !has {
			t.Errorf("cache not exist '%s',but want", key)
		}
		key = "a"
		if has, _ := cache.Has(key); has {
			t.Errorf("cache exist '%s',but not want", key)
		}

		// improve b priorityQueue
		// {"d", "e", "b"}
		key = "b"
		if has, _ := cache.Has(key); !has {
			t.Errorf("cache not exist '%s',but want", key)
		}

		// add f
		// eviction d -> {"e", "b", "f"}
		key = "f"
		items = append(items, key)
		if v, err := cache.GetLoad(key, func(k Key) (Value, error) {
			return strings.ToUpper(k), nil
		}); err == nil || err == NotFound {
			log.Infof("GetLoad() %v=%v", key, v)
		} else {
			t.Errorf("GetLoad() %v error = %v", key, err)
		}
		if has, _ := cache.Has(key); !has {
			t.Errorf("cache not exist '%s',but want", key)
		}
		key = "d"
		if has, _ := cache.Has(key); has {
			t.Errorf("cache exist '%s',but not want", key)
		}
	})
}

func Test_safeLRUCache_Expire(t *testing.T) {
	// Test
	t.Run("Test_safeLRUCache_Expire", func(t *testing.T) {
		cache := NewSafe(100, time.Second, LRUPriorityFunc)
		defer cache.Close()
		items := []string{"a", "b", "c", "d", "e", "f", "g"}
		// {"a", "b", "c"}
		for _, i := range items {
			if err := cache.Set(i, strings.ToUpper(i)); err != nil {
				t.Errorf("Set() error = %v", err)
			}
		}
		if cache.Len() != len(items) {
			t.Errorf("cache len is %d want %d", cache.Len(), len(items))
		}
		for _, i := range items {
			if has, _ := cache.Has(i); !has {
				t.Errorf("cache not exist '%s',but want", i)
			}
		}

		// sleep 2s
		time.Sleep(time.Second * 2)

		if cache.Len() != 0 {
			t.Errorf("cache len is %d want %d", cache.Len(), 0)
		}
		for _, i := range items {
			if has, _ := cache.Has(i); has {
				t.Errorf("cache exist '%s',but not want", i)
			}
		}
	})
}
