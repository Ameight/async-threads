package async_process

import "context"

type Thread struct {
	// Name is thread name for graph visualization
	Name string

	// started flag is responsible for
	started bool

	// ended flag is responsible for
	ended bool

	// success
	success bool

	// DependsOn
	DependsOn []*Thread

	// Channel
	Channel chan error

	// Action
	Action func(context.Context) chan error
}

// canStart return `true` if Thread can start.
func (t *Thread) canStart() bool {
	if t.started || t.ended {
		return false
	}

	for _, dependentThread := range t.DependsOn {
		if !dependentThread.success {
			return false
		}
	}

	return true
}

func (t *Thread) start(ctx context.Context) {
	t.Channel = t.Action(ctx)
}
