package workSteal

import (
	"sync/atomic"
	"unsafe"
)

// A struct to hold the value and the stamp
type StampedValue struct {
	value uint64
	stamp uint64
}

// A DEQueue struct to hold the top pointer to a struct holding the value and the stamp,
// the bottom index, and the tasks
type DEQueue struct {
	top    unsafe.Pointer
	bottom uint64
	tasks  []any
}

// A function to create a new DEQueue with the given size
func NewDEQueue(size uint64) *DEQueue {
	top := StampedValue{value: 0, stamp: 0}
	q := &DEQueue{
		top:    unsafe.Pointer(&top),
		bottom: 0,
		tasks:  make([]any, size),
	}
	return q
}

// A function for owner to push a task to the bottom of the DEQueue
func (q *DEQueue) PushBottom(r any) {
	// Push the task to the bottom of the DEQueue
	q.tasks[atomicLoadBottom(&q.bottom)] = r
	// Increment the bottom index
	atomicAddBottom(&q.bottom, 1)
}

// A function for stealer to attempt to pop a task from the top of the DEQueue
func (q *DEQueue) PopTop() any {
	// Atomically load the top pointer for the stamp and the top index
	ptr := atomicLoadStampedValue(&q.top)
	oldTop := ptr.value
	oldStamp := ptr.stamp
	// Calculate the new top index and the new stamp
	newTop := oldTop + 1
	newStamp := oldStamp + 1
	// If the bottom index is less than or equal to the old top index, return nil
	if atomicLoadBottom(&q.bottom) <= oldTop {
		return nil
	}
	// Get the task from the old top index
	r := q.tasks[oldTop]
	// Create a new StampedValue with the new top index and the new stamp
	// and try to compare and swap the top pointer with the new StampedValue
	if cas(&q.top, ptr, &StampedValue{value: newTop, stamp: newStamp}) {
		// If the compare and swap was successful, the stealer gets the task
		return r
	}
	return nil
}

// A function for owner to attempt to pop a task from the bottom of the DEQueue
func (q *DEQueue) PopBottom() any {
	// If the bottom index is 0, there is no task to pop, return nil
	if atomicLoadBottom(&q.bottom) == 0 {
		return nil
	}
	// Decrement the bottom index
	atomicAddBottom(&q.bottom, ^uint64(0))
	// Get the task from the new bottom index
	r := q.tasks[atomicLoadBottom(&q.bottom)]
	// Atomically load the top pointer for the stamp and the top index
	ptr := atomicLoadStampedValue(&q.top)
	oldTop := ptr.value
	oldStamp := ptr.stamp
	// Calculate the new top index and the new stamp
	newTop := uint64(0)
	newStamp := oldStamp + 1
	// If the bottom index is less than the old top index, the owner must get the task
	if atomicLoadBottom(&q.bottom) > oldTop {
		// Return the task to the owner
		return r
	}
	// Create a new StampedValue with the new top index and the new stamp
	newStampedValue := &StampedValue{value: newTop, stamp: newStamp}
	// If the bottom index is equal to the old top index, the owner must compete with the stealer
	// for the last remaining task
	if atomicLoadBottom(&q.bottom) == oldTop {
		// Update the bottom index to 0
		atomicStoreBottom(&q.bottom, 0)
		// Try to compare and swap the top pointer with the new StampedValue
		if cas(&q.top, ptr, newStampedValue) {
			// If the compare and swap was successful, the owner gets the task
			return r
		}
	}
	// The owner lost, bottom could be less than top, reset the bottom index as now the
	// DEQueue is empty
	atomicStoreBottom(&q.bottom, 0)
	return nil
}

// Helper functions to perform atomic operations and compare and swap

func atomicLoadBottom(ptr *uint64) uint64 {
	return atomic.LoadUint64(ptr)
}

func atomicStoreBottom(ptr *uint64, val uint64) {
	atomic.StoreUint64(ptr, val)
}

func atomicAddBottom(ptr *uint64, val uint64) {
	atomic.AddUint64(ptr, val)
}

func atomicLoadStampedValue(ptr *unsafe.Pointer) *StampedValue {
	return (*StampedValue)(atomic.LoadPointer(ptr))
}

func cas(ptr *unsafe.Pointer, old, new *StampedValue) bool {
	return atomic.CompareAndSwapPointer(ptr, unsafe.Pointer(old), unsafe.Pointer(new))
}
