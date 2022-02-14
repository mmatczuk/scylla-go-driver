package transport

// Dummy items used because they are still not incorporated to main.

type ConnPool interface{}

func InitNodeConnPool(addr string, cfg ConnConfig) *ConnPool {
	return (*ConnPool)(nil)
}

type Node struct {
	addr       string
	datacenter string
	rack       string

	pool *ConnPool
}
