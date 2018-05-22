package control

import "container/heap"

// priorityQueue implements heap.Interface and holds Query objects.
type priorityQueue []*Query

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].spec.Resources.Priority < pq[j].spec.Resources.Priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	q := x.(*Query)
	*pq = append(*pq, q)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	q := old[n-1]
	*pq = old[0 : n-1]
	return q
}

type PriorityQueue struct {
	queue priorityQueue
}

func newPriorityQueue() *PriorityQueue {
	return &PriorityQueue{
		queue: make(priorityQueue, 0, 100),
	}
}

func (p *PriorityQueue) Push(q *Query) {
	heap.Push(&p.queue, q)
}

func (p *PriorityQueue) Peek() *Query {
	for {
		if p.queue.Len() == 0 {
			return nil
		}
		q := p.queue[0]
		if q.isOK() {
			return q
		}
		heap.Pop(&p.queue)
	}
}
func (p *PriorityQueue) Pop() *Query {
	for {
		if p.queue.Len() == 0 {
			return nil
		}
		q := heap.Pop(&p.queue).(*Query)
		if q.isOK() {
			return q
		}
	}
}
