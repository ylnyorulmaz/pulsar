//go:build !windows

package runner

import (
	"os"
	"os/exec"
	"syscall"
	"time"
)

func configureCmd(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		return cmd.Process.Kill()
	}
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	time.Sleep(2 * time.Second)
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	return nil
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
