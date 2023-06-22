package lock

import (
	"time"
)

type item struct {
	callback func()
	priority int64
	resource string
	id       string
	ttl      int
}

func newItem(res, id string, ttl int, onExpire func()) item {
	return item{
		callback: onExpire,
		resource: res,
		id:       id,
		priority: time.Now().Add(time.Second * time.Duration(ttl)).Unix(),
	}
}

func (i item) ID() string {
	return i.id
}

func (i item) Priority() int64 {
	return i.priority
}

// TODO realize more performant way to delete 1 item
// GroupID uses id to delete only 1 item (they all have unique ID's)
func (i item) GroupID() string {
	return i.id
}

func (i item) TTL() int {
	return i.ttl
}
