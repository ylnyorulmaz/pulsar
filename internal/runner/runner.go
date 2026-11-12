package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
	"github.com/shirou/gopsutil/v3/process"

	"github.com/ylnyorulmaz/pulsar/internal/protocol"
	"github.com/ylnyorulmaz/pulsar/internal/redact"
	"github.com/ylnyorulmaz/pulsar/internal/ringbuffer"
)

// Config holds CLI options for pulse run.
type Config struct {
	Token    string
	Name     string
	AdminURL string
	HubURL   string
	Command  []string
}

type hubConn struct {
	mu   sync.Mutex
	conn *websocket.Conn
	id   string
}

func (h *hubConn) set(c *websocket.Conn, id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn != nil {
		_ = h.conn.Close()
	}
	h.conn = c
	h.id = id
}

func (h *hubConn) write(v any) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn == nil {
		return fmt.Errorf("not connected to hub")
	}
	return h.conn.WriteJSON(v)
}

func (h *hubConn) runnerID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.id
}

func dialHub(cfg Config) (*websocket.Conn, string, error) {
	u, err := url.Parse(cfg.HubURL)
	if err != nil {
		return nil, "", err
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http", "ws":
		u.Scheme = "ws"
	case "wss":
		// ok
	default:
		return nil, "", fmt.Errorf("unsupported hub URL scheme %q", u.Scheme)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/ws/runner"

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+cfg.Token)

	c, resp, err := dialer.Dial(u.String(), hdr)
	if err != nil {
		if resp != nil {
			return nil, "", fmt.Errorf("dial hub: %w (HTTP %d)", err, resp.StatusCode)
		}
		return nil, "", fmt.Errorf("dial hub: %w", err)
	}

	reg := protocol.RegisterMessage{
		Type:     protocol.TypeRegister,
		Token:    cfg.Token,
		Name:     cfg.Name,
		AdminURL: cfg.AdminURL,
		Host:     hostname(),
	}
	if err := c.WriteJSON(reg); err != nil {
		c.Close()
		return nil, "", err
	}

	var ack protocol.RegisteredMessage
	if err := c.ReadJSON(&ack); err != nil {
		c.Close()
		return nil, "", err
	}
	if ack.Type != protocol.TypeRegistered || ack.RunnerID == "" {
		c.Close()
		return nil, "", fmt.Errorf("hub registration failed")
	}
	return c, ack.RunnerID, nil
}

// Run wraps command in a PTY, streams to hub, and handles remote KILL.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Token == "" {
		return fmt.Errorf("--token is required")
	}
	if len(cfg.Command) == 0 {
		return fmt.Errorf("command is required after --")
	}
	if cfg.Name == "" {
		cfg.Name = cfg.Command[0]
	}
	if cfg.HubURL == "" {
		cfg.HubURL = os.Getenv("PULSE_HUB_URL")
	}
	if cfg.HubURL == "" {
		cfg.HubURL = "http://127.0.0.1:8741"
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctx, cfg.Command[0], cfg.Command[1:]...)
	configureCmd(cmd)
	cmd.Env = os.Environ()

	ptmx, err := pty.Start(cmd)
	if err != nil {
		return fmt.Errorf("pty start: %w", err)
	}
	defer ptmx.Close()

	var killed atomic.Bool
	started := time.Now()
	ring := ringbuffer.New(500)
	var lineCarry string
	var hc hubConn

	sendOutput := func(data []byte) {
		clean := string(redact.Bytes(data))
		ring.Append(clean, &lineCarry)
		id := hc.runnerID()
		if id == "" {
			return
		}
		_ = hc.write(protocol.OutputMessage{
			Type:     protocol.TypeOutput,
			RunnerID: id,
			Data:     clean,
		})
	}

	sendTelemetry := func(status string) {
		id := hc.runnerID()
		if id == "" {
			return
		}
		cpu, mem := processStats(cmd.Process.Pid)
		_ = hc.write(protocol.TelemetryMessage{
			Type:     protocol.TypeTelemetry,
			RunnerID: id,
			Name:     cfg.Name,
			Host:     hostname(),
			Status:   status,
			Uptime:   int64(time.Since(started).Seconds()),
			CPUPct:   cpu,
			MemMB:    mem,
			AdminURL: cfg.AdminURL,
			PID:      cmd.Process.Pid,
		})
	}

	handleControl := func(raw []byte) {
		var env protocol.Envelope
		if err := json.Unmarshal(raw, &env); err != nil || env.Type != protocol.TypeControl {
			return
		}
		var ctl protocol.ControlMessage
		if err := json.Unmarshal(raw, &ctl); err != nil {
			return
		}
		if ctl.Action != protocol.ControlKill {
			log.Printf("pulse: ignoring unknown control action %q", ctl.Action)
			return
		}
		killed.Store(true)
		_ = hc.write(protocol.StatusMessage{
			Type:     protocol.TypeStatus,
			RunnerID: hc.runnerID(),
			Status:   protocol.StatusKilled,
		})
		_ = killProcessGroup(cmd)
		cancel()
	}

	sendCatchUp := func() {
		id := hc.runnerID()
		if id == "" {
			return
		}
		lines := ring.Snapshot()
		if len(lines) == 0 {
			return
		}
		_ = hc.write(protocol.CatchUpMessage{
			Type:     protocol.TypeCatchUp,
			RunnerID: id,
			Lines:    lines,
		})
	}

	// Hub WebSocket with reconnect
	hubDone := make(chan struct{})
	go func() {
		defer close(hubDone)
		backoff := time.Second
		for {
			if ctx.Err() != nil && hc.runnerID() == "" {
				return
			}
			c, id, err := dialHub(cfg)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("pulse: hub: %v (retry in %s)", err, backoff)
				time.Sleep(backoff)
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}
			backoff = time.Second
			hc.set(c, id)
			log.Printf("pulse: connected to hub as %s", id)
			sendCatchUp()
			sendTelemetry(protocol.StatusRunning)

			for {
				_, raw, err := c.ReadMessage()
				if err != nil {
					log.Printf("pulse: hub read: %v", err)
					hc.set(nil, id) // keep id for telemetry attempts until reconnect
					_ = c.Close()
					break
				}
				handleControl(raw)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// PTY → hub
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				sendOutput(buf[:n])
			}
			if err != nil {
				if err != io.EOF {
					log.Printf("pulse: pty read: %v", err)
				}
				return
			}
		}
	}()

	// Periodic telemetry
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				st := protocol.StatusRunning
				if killed.Load() {
					st = protocol.StatusKilled
				}
				sendTelemetry(st)
			}
		}
	}()

	// Local SIGINT still works
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-sigCh:
			_ = killProcessGroup(cmd)
			cancel()
		case <-ctx.Done():
		}
	}()

	waitErr := waitProcess(cmd)
	ring.Flush(lineCarry)
	sendCatchUp()

	exitCode := 0
	status := protocol.StatusExited
	if killed.Load() {
		status = protocol.StatusKilled
	} else if waitErr != nil {
		if ee, ok := waitErr.(*exec.ExitError); ok {
			exitCode = ee.ExitCode()
		}
		status = protocol.StatusExited
	}

	_ = hc.write(protocol.StatusMessage{
		Type:     protocol.TypeStatus,
		RunnerID: hc.runnerID(),
		Status:   status,
		ExitCode: exitCode,
	})
	sendTelemetry(status)

	cancel()
	<-hubDone

	if killed.Load() {
		return nil
	}
	return waitErr
}

func processStats(pid int) (cpuPct float64, memMB float64) {
	if pid <= 0 {
		return 0, 0
	}
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return 0, 0
	}
	if cpu, err := p.CPUPercent(); err == nil {
		cpuPct = cpu
	}
	if mem, err := p.MemoryInfo(); err == nil && mem != nil {
		memMB = float64(mem.RSS) / (1024 * 1024)
	}
	return cpuPct, memMB
}
