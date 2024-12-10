package workSteal_test

import (
	"parcompress/workSteal"
	"sync"
	"testing"
)

// Test simple push and pop bottom operations
func TestDEQueuePushAndPopBottom(t *testing.T) {
	q := workSteal.NewDEQueue(5)

	// Test empty queue pop bottom
	if result := q.PopBottom(); result != nil {
		t.Errorf("Expected nil, got %v", result)
	}

	// Test queue with one element
	q.PushBottom(1)
	if result := q.PopBottom(); result != 1 {
		t.Errorf("Expected 1, got %v", result)
	}

	// Test when the queue with more than 1 elements
	q.PushBottom(1)
	q.PushBottom(2)
	if result := q.PopBottom(); result != 2 {
		t.Errorf("Expected 3, got %v", result)
	}
	if result := q.PopBottom(); result != 1 {
		t.Errorf("Expected 2, got %v", result)
	}
	if result := q.PopBottom(); result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}

// Test simple pop top operations
func TestDEQueuePopTop(t *testing.T) {
	q := workSteal.NewDEQueue(5)

	// Test empty queue pop top
	if result := q.PopTop(); result != nil {
		t.Errorf("Expected nil, got %v", result)
	}

	// Test queue with one element
	q.PushBottom(1)
	if result := q.PopTop(); result != 1 {
		t.Errorf("Expected 1, got %v", result)
	}

	// Test when the queue with more than 1 elements
	q.PushBottom(1)
	q.PushBottom(2)
	if result := q.PopTop(); result != 1 {
		t.Errorf("Expected 1, got %v", result)
	}
	if result := q.PopTop(); result != 2 {
		t.Errorf("Expected 2, got %v", result)
	}
	if result := q.PopTop(); result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}

// Test 1 owner and 1 stealer with conflict on last task
func TestDEQueueOwnerSingleStealerConflict(t *testing.T) {
	// Number of runs
	runs := 10000
	// Size of the queue
	queueSize := 1000
	// Run the test multiple times
	for i := 0; i < runs; i++ {
		// Create a new DEQueue of the given size
		q := workSteal.NewDEQueue(uint64(queueSize))

		// Push some tasks to the queue
		for i := 0; i < queueSize; i++ {
			q.PushBottom(i + 1)
		}

		// Create a WaitGroup to wait for the goroutines to finish
		var wg sync.WaitGroup
		wg.Add(2)

		// Start a stealer goroutine to pop tasks from the top of the queue
		stealer := []int{}
		go func() {
			defer wg.Done()
			for {
				// Pop the task from the top of the queue
				stealerRes := q.PopTop()
				// If there is no more task, break the loop
				if stealerRes == nil {
					break
				}
				// Append the task to the stealer slice
				stealer = append(stealer, stealerRes.(int))
			}
		}()

		// Start an owner goroutine to pop tasks from the bottom of the queue
		owner := []int{}
		go func() {
			defer wg.Done()
			for {
				// Pop the task from the bottom of the queue
				ownerRes := q.PopBottom()
				// If there is no more task, break the loop
				if ownerRes == nil {
					break
				}
				// Append the task to the owner slice
				owner = append(owner, ownerRes.(int))
			}
		}()

		// Wait for the goroutines to finish
		wg.Wait()

		// Check that the owner and stealer have mutually exclusive tasks
		for _, o := range owner {
			for _, s := range stealer {
				if o == s {
					t.Errorf("Expected owner and stealer to have mutually exclusive tasks %v != %v", o, s)
				}
			}
		}

		// Check that the total number of tasks equals the size
		if len(owner)+len(stealer) != queueSize {
			t.Errorf("Expected %d, got %d", queueSize, len(owner)+len(stealer))
		}

		// Check that the queue is empty
		if result := q.PopBottom(); result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	}
}

// Test 1 owner and multiple stealers trying to steal tasks from the owner with conflict
func TestDEQueueOwnerMultiStealersConflict(t *testing.T) {
	// Number of runs
	runs := 10000
	// Size of the queue
	queueSize := 1000
	// Number of stealers
	stealerCount := 9
	// Run the test multiple times
	for i := 0; i < runs; i++ {
		// Create a new DEQueue of the given size
		q := workSteal.NewDEQueue(uint64(queueSize))

		// Push some tasks to the queue
		for i := 0; i < queueSize; i++ {
			q.PushBottom(i + 1)
		}

		// Create a WaitGroup to wait for the goroutines to finish
		var wg sync.WaitGroup
		wg.Add(stealerCount + 1)

		// Start stealer goroutines to pop tasks from the top of the queue
		stealers := make([][]int, stealerCount)
		for i := 0; i < 9; i++ {
			i := i
			go func() {
				defer wg.Done()
				for {
					// Pop the task from the top of the queue
					stealerRes := q.PopTop()
					// If there is no more task, break the loop
					if stealerRes == nil {
						break
					}
					// Append the task to the stealer slice
					stealers[i] = append(stealers[i], stealerRes.(int))
				}
			}()
		}

		// Start an owner goroutine to pop tasks from the bottom of the queue
		owner := []int{}
		go func() {
			defer wg.Done()
			for {
				// Pop the task from the bottom of the queue
				ownerRes := q.PopBottom()
				// If there is no more task, break the loop
				if ownerRes == nil {
					break
				}
				// Append the task to the owner slice
				owner = append(owner, ownerRes.(int))
			}
		}()

		// Wait for the goroutines to finish
		wg.Wait()

		// Check that the owner and stealers all have mutually exclusive tasks
		tasks := make(map[any]bool)
		for _, o := range owner {
			if tasks[o] {
				t.Errorf("Task %v is not exclusive in owner", o)
			}
			tasks[o] = true
		}
		for _, stealer := range stealers {
			for _, s := range stealer {
				if tasks[s] {
					t.Errorf("Task %v is not exclusive in stealer %v", s, stealer)
				}
				tasks[s] = true
			}
		}

		// Check that the total number of tasks equals the queuesize
		totalInStealers := 0
		for _, s := range stealers {
			totalInStealers += len(s)
		}
		combinedTasksTaken := len(owner) + totalInStealers
		if combinedTasksTaken != queueSize {
			t.Errorf("Expected %d, got %d", queueSize, combinedTasksTaken)
		}

		// Check that the queue is empty
		if result := q.PopBottom(); result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	}
}
