package async_process

import (
	"context"

	"github.com/hashicorp/go-multierror"
)

func Run(ctx context.Context, threadConfig map[int]*Thread, masterChannel chan error) {
	var receivedError *multierror.Error
	threads := threadConfig

	for {
		allStartedFinished := true
		successAll := true
		for _, thread := range threads {
			allStartedFinished = allStartedFinished && (thread.started == thread.ended)
			successAll = successAll && thread.success
		}

		for i := range threads {
			select {
			case <-ctx.Done():
				return
			case err := <-threads[i].Channel:
				threads[i].ended = true
				threads[i].success = err == nil
				if err != nil {
					receivedError = multierror.Append(receivedError, err)
				}
			default:
				continue
			}
		}

		switch {

		case successAll:
			masterChannel <- nil
			return

		case receivedError != nil && allStartedFinished:
			masterChannel <- receivedError.ErrorOrNil()
			return

		default:
			for _, thread := range threads {
				if thread.canStart() {
					thread.start(ctx)
					thread.started = true
				}
			}
		}
	}
}
