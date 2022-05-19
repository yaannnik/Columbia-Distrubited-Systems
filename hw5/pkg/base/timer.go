package base

// Timer An interface for timer in a node
type Timer interface {

	// RemainingTime Check how long will this timer be triggered.
	RemainingTime() int

	// Wait Let the timer elapse. All the timers in a node should be called Wait with the same input.
	Wait(t int)
}
