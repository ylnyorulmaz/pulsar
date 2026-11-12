//go:build windows

package runner

import (
	"os"
	"os/exec"
	"syscall"
	"time"
)

func configureCmd(cmd *exec.Cmd) {
	// No POSIX process groups on Windows; Job objects would be ideal but we keep a simple tree kill.
}

func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	time.Sleep(2 * time.Second)
	return cmd.Process.Kill()
}

func waitProcess(cmd *exec.Cmd) error {
	return cmd.Wait()
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}
