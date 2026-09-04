package engine

// deque is a FIFO used for both priority bands and group message lists.
type deque[T any] struct {
	buf  []T
	head int
}

func (d *deque[T]) len() int    { return len(d.buf) - d.head }
func (d *deque[T]) empty() bool { return d.len() == 0 }

func (d *deque[T]) pushBack(v T) { d.buf = append(d.buf, v) }

func (d *deque[T]) pushFront(v T) {
	if d.head > 0 {
		d.head--
		d.buf[d.head] = v
		return
	}
	d.buf = append(d.buf, v)
	copy(d.buf[1:], d.buf[:len(d.buf)-1])
	d.buf[0] = v
}

func (d *deque[T]) popFront() (T, bool) {
	var zero T
	if d.head == len(d.buf) {
		return zero, false
	}
	v := d.buf[d.head]
	d.buf[d.head] = zero // release the reference so it can be collected
	d.head++
	if d.head > 32 && d.head*2 >= len(d.buf) {
		d.buf = append(d.buf[:0], d.buf[d.head:]...)
		d.head = 0
	}
	return v, true
}

func (d *deque[T]) front() (T, bool) {
	var zero T
	if d.head == len(d.buf) {
		return zero, false
	}
	return d.buf[d.head], true
}

func (d *deque[T]) at(i int) T { return d.buf[d.head+i] }

func (d *deque[T]) all() []T {
	out := make([]T, d.len())
	copy(out, d.buf[d.head:])
	return out
}

func (d *deque[T]) prepend(vs []T) {
	if len(vs) == 0 {
		return
	}
	buf := make([]T, 0, len(vs)+d.len())
	buf = append(buf, vs...)
	buf = append(buf, d.buf[d.head:]...)
	d.buf, d.head = buf, 0
}
