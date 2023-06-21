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

func (i item) Priority() int64 {
	return i.priority
}

func (i item) Resource() string {
	return i.resource
}

func (i item) ID() string {
	return i.id
}

// TODO: use resource as GroupID
func (i item) GroupID() string {
	return ""
}

func (i item) TTL() int {
	return i.ttl
}
