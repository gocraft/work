// +build !windows

package workers

import (
	"os"
	"os/signal"
	"syscall"
)

func handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGTERM)

	for sig := range signals {
		switch sig {
		case syscall.SIGINT, syscall.SIGUSR1, syscall.SIGTERM:
			Quit()
		}
	}
}
