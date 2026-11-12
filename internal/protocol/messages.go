// Package protocol defines the wire format between hub, runners, and dashboards.
// Control messages are strict enums only — runners never execute arbitrary shell from the server.
package protocol

// MessageType discriminates WebSocket envelope payloads.
type MessageType string

const (
	TypeRegister     MessageType = "register"
	TypeRegistered   MessageType = "registered"
	TypeOutput       MessageType = "output"
	TypeCatchUp      MessageType = "catchup"
	TypeTelemetry    MessageType = "telemetry"
	TypeRunnerList   MessageType = "runner_list"
	TypeSubscribe    MessageType = "subscribe"
	TypeKill         MessageType = "kill"
	TypeControl      MessageType = "control"
	TypeStatus       MessageType = "status"
	TypeError        MessageType = "error"
	TypePing         MessageType = "ping"
	TypePong         MessageType = "pong"
)

// Runner status values shown on the dashboard.
const (
	StatusRunning = "RUNNING"
	StatusIdle    = "IDLE"
	StatusKilled  = "KILLED"
	StatusExited  = "EXITED"
)

// Envelope wraps every JSON message on the wire.
type Envelope struct {
	Type MessageType `json:"type"`
}

// RegisterMessage is sent by a runner immediately after connecting.
type RegisterMessage struct {
	Type      MessageType `json:"type"`
	Token     string      `json:"token"`
	Name      string      `json:"name"`
	AdminURL  string      `json:"admin_url,omitempty"`
	Host      string      `json:"host,omitempty"`
	PID       int         `json:"pid,omitempty"`
}

// RegisteredMessage confirms runner identity assigned by the hub.
type RegisteredMessage struct {
	Type     MessageType `json:"type"`
	RunnerID string      `json:"runner_id"`
}

// OutputMessage carries a chunk of PTY output (already redacted on the runner).
type OutputMessage struct {
	Type     MessageType `json:"type"`
	RunnerID string      `json:"runner_id"`
	Data     string      `json:"data"`
}

// CatchUpMessage replays ring-buffer lines after reconnect.
type CatchUpMessage struct {
	Type     MessageType `json:"type"`
	RunnerID string      `json:"runner_id"`
	Lines    []string    `json:"lines"`
}

// TelemetryMessage reports live resource usage for one runner.
type TelemetryMessage struct {
	Type     MessageType `json:"type"`
	RunnerID string      `json:"runner_id"`
	Name     string      `json:"name"`
	Host     string      `json:"host"`
	Status   string      `json:"status"`
	Uptime   int64       `json:"uptime_sec"`
	CPUPct   float64     `json:"cpu_pct"`
	MemMB    float64     `json:"mem_mb"`
	AdminURL string      `json:"admin_url,omitempty"`
	PID      int         `json:"pid,omitempty"`
}

// RunnerInfo is the dashboard-facing snapshot of a runner.
type RunnerInfo struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Host     string  `json:"host"`
	Status   string  `json:"status"`
	Uptime   int64   `json:"uptime_sec"`
	CPUPct   float64 `json:"cpu_pct"`
	MemMB    float64 `json:"mem_mb"`
	AdminURL string  `json:"admin_url,omitempty"`
	PID      int     `json:"pid,omitempty"`
}

// RunnerListMessage broadcasts the current registry to dashboards.
type RunnerListMessage struct {
	Type    MessageType `json:"type"`
	Runners []RunnerInfo `json:"runners"`
}

// SubscribeMessage selects which runner's output a dashboard client wants.
type SubscribeMessage struct {
	Type     MessageType `json:"type"`
	RunnerID string      `json:"runner_id"`
	Token    string      `json:"token,omitempty"`
}

// KillRequest is initiated by the dashboard; hub forwards ControlMessage to runner.
type KillRequest struct {
	Type     MessageType `json:"type"`
	RunnerID string      `json:"runner_id"`
	Token    string      `json:"token"`
}

// ControlAction is the only server→runner control vocabulary.
type ControlAction string

const ControlKill ControlAction = "KILL"

// ControlMessage is the sole control payload accepted by the runner.
type ControlMessage struct {
	Type   MessageType   `json:"type"`
	Action ControlAction `json:"action"`
}

// StatusMessage updates runner lifecycle state.
type StatusMessage struct {
	Type     MessageType `json:"type"`
	RunnerID string      `json:"runner_id"`
	Status   string      `json:"status"`
	ExitCode int         `json:"exit_code,omitempty"`
}

// ErrorMessage reports auth or protocol failures.
type ErrorMessage struct {
	Type    MessageType `json:"type"`
	Message string      `json:"message"`
}
