package hub

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ylnyorulmaz/pulsar/internal/protocol"
)

const (
	defaultReadLimit = 512 * 1024
	pongWait         = 60 * time.Second
	pingPeriod       = (pongWait * 9) / 10
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		// Self-hosted: allow same-origin and local dev; tighten behind your reverse proxy in production.
		return true
	},
}

// Config for the Pulse hub HTTP server.
type Config struct {
	Addr   string
	Secret string
}

// Server is the central registry and broadcast fabric.
type Server struct {
	cfg      Config
	mu       sync.RWMutex
	runners  map[string]*runnerSession
	dash     map[*dashboardClient]struct{}
	runnerWG sync.WaitGroup
}

type runnerSession struct {
	id       string
	info     protocol.RunnerInfo
	conn     *websocket.Conn
	send     chan []byte
	lastTele time.Time
	lines    []string
	lineCap  int
	partial  string
}

type dashboardClient struct {
	conn         *websocket.Conn
	send         chan []byte
	subscribedTo string
	token        string
}

// New constructs a Hub server.
func New(cfg Config) *Server {
	return &Server{
		cfg:     cfg,
		runners: make(map[string]*runnerSession),
		dash:    make(map[*dashboardClient]struct{}),
	}
}

func (s *Server) handleRunnerWS(w http.ResponseWriter, r *http.Request) {
	if !s.authToken(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c.SetReadLimit(defaultReadLimit)
	_ = c.SetReadDeadline(time.Now().Add(pongWait))
	c.SetPongHandler(func(string) error {
		return c.SetReadDeadline(time.Now().Add(pongWait))
	})

	var reg protocol.RegisterMessage
	if err := c.ReadJSON(&reg); err != nil || reg.Type != protocol.TypeRegister {
		_ = c.WriteJSON(protocol.ErrorMessage{Type: protocol.TypeError, Message: "expected register"})
		c.Close()
		return
	}
	if reg.Token != s.cfg.Secret {
		_ = c.WriteJSON(protocol.ErrorMessage{Type: protocol.TypeError, Message: "invalid token"})
		c.Close()
		return
	}

	id := newID()
	sess := &runnerSession{
		id: id,
		info: protocol.RunnerInfo{
			ID:       id,
			Name:     reg.Name,
			Host:     reg.Host,
			Status:   protocol.StatusRunning,
			AdminURL: reg.AdminURL,
			PID:      reg.PID,
		},
		conn:    c,
		send:    make(chan []byte, 256),
		lineCap: 500,
	}
	s.mu.Lock()
	s.runners[id] = sess
	s.mu.Unlock()

	_ = c.WriteJSON(protocol.RegisteredMessage{Type: protocol.TypeRegistered, RunnerID: id})
	s.broadcastRunnerList()

	s.runnerWG.Add(1)
	go s.runnerWritePump(sess)
	go s.runnerReadPump(sess)
}

func (s *Server) runnerReadPump(sess *runnerSession) {
	defer func() {
		s.runnerWG.Done()
		s.removeRunner(sess.id)
		sess.conn.Close()
	}()
	for {
		_, raw, err := sess.conn.ReadMessage()
		if err != nil {
			return
		}
		s.handleRunnerMessage(sess, raw)
	}
}

func (s *Server) handleRunnerMessage(sess *runnerSession, raw []byte) {
	var env protocol.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return
	}

	switch env.Type {
	case protocol.TypeOutput:
		var o protocol.OutputMessage
		if json.Unmarshal(raw, &o) == nil {
			sess.appendOutput(o.Data)
		}
		s.fanoutToDashboards(raw)
	case protocol.TypeCatchUp:
		var cu protocol.CatchUpMessage
		if json.Unmarshal(raw, &cu) == nil {
			sess.lines = append([]string(nil), cu.Lines...)
			if len(sess.lines) > sess.lineCap {
				sess.lines = sess.lines[len(sess.lines)-sess.lineCap:]
			}
			sess.partial = ""
		}
		s.fanoutToDashboards(raw)
	case protocol.TypeTelemetry:
		var t protocol.TelemetryMessage
		if json.Unmarshal(raw, &t) == nil {
			s.mu.Lock()
			if r, ok := s.runners[sess.id]; ok {
				r.info.Name = t.Name
				r.info.Host = t.Host
				r.info.Status = t.Status
				r.info.Uptime = t.Uptime
				r.info.CPUPct = t.CPUPct
				r.info.MemMB = t.MemMB
				r.info.AdminURL = t.AdminURL
				r.info.PID = t.PID
				r.lastTele = time.Now()
			}
			s.mu.Unlock()
			s.broadcastRunnerList()
			s.fanoutToDashboards(raw)
		}
	case protocol.TypeStatus:
		var st protocol.StatusMessage
		if json.Unmarshal(raw, &st) == nil {
			s.mu.Lock()
			if r, ok := s.runners[sess.id]; ok {
				r.info.Status = st.Status
			}
			s.mu.Unlock()
			s.broadcastRunnerList()
			s.fanoutToDashboards(raw)
		}
	case protocol.TypePing:
		_ = sess.conn.WriteJSON(protocol.Envelope{Type: protocol.TypePong})
	}
}

func (s *Server) runnerWritePump(sess *runnerSession) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-sess.send:
			if !ok {
				return
			}
			if err := sess.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			if err := sess.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (s *Server) sendControl(sess *runnerSession, action protocol.ControlAction) error {
	msg := protocol.ControlMessage{Type: protocol.TypeControl, Action: action}
	return sess.conn.WriteJSON(msg)
}

func (s *Server) handleDashboardWS(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c.SetReadLimit(defaultReadLimit)

	dc := &dashboardClient{conn: c, send: make(chan []byte, 256)}
	s.mu.Lock()
	s.dash[dc] = struct{}{}
	s.mu.Unlock()

	_ = c.WriteJSON(s.runnerListMessage())

	go s.dashboardWritePump(dc)
	s.dashboardReadPump(dc)
}

func (s *Server) dashboardReadPump(dc *dashboardClient) {
	defer func() {
		s.mu.Lock()
		delete(s.dash, dc)
		s.mu.Unlock()
		dc.conn.Close()
	}()
	for {
		_, raw, err := dc.conn.ReadMessage()
		if err != nil {
			return
		}
		var env protocol.Envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			continue
		}
		switch env.Type {
		case protocol.TypeSubscribe:
			var sub protocol.SubscribeMessage
			if json.Unmarshal(raw, &sub) == nil {
				dc.subscribedTo = sub.RunnerID
				s.mu.RLock()
				sess, ok := s.runners[sub.RunnerID]
				s.mu.RUnlock()
				if ok && len(sess.lines) > 0 {
					replay, _ := json.Marshal(protocol.CatchUpMessage{
						Type:     protocol.TypeCatchUp,
						RunnerID: sub.RunnerID,
						Lines:    append([]string(nil), sess.lines...),
					})
					select {
					case dc.send <- replay:
					default:
					}
				}
			}
		case protocol.TypeKill:
			var kr protocol.KillRequest
			if json.Unmarshal(raw, &kr) != nil {
				continue
			}
			if kr.Token != s.cfg.Secret {
				_ = dc.conn.WriteJSON(protocol.ErrorMessage{Type: protocol.TypeError, Message: "invalid kill token"})
				continue
			}
			s.mu.RLock()
			sess, ok := s.runners[kr.RunnerID]
			s.mu.RUnlock()
			if !ok {
				continue
			}
			if err := s.sendControl(sess, protocol.ControlKill); err != nil {
				log.Printf("pulse: kill forward: %v", err)
			}
		}
	}
}

func (s *Server) dashboardWritePump(dc *dashboardClient) {
	for msg := range dc.send {
		if err := dc.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}

func (s *Server) fanoutToDashboards(raw []byte) {
	var env protocol.Envelope
	_ = json.Unmarshal(raw, &env)

	s.mu.RLock()
	defer s.mu.RUnlock()
	for dc := range s.dash {
		if env.Type == protocol.TypeOutput || env.Type == protocol.TypeCatchUp {
			var rid string
			if env.Type == protocol.TypeOutput {
				var o protocol.OutputMessage
				_ = json.Unmarshal(raw, &o)
				rid = o.RunnerID
			} else {
				var o protocol.CatchUpMessage
				_ = json.Unmarshal(raw, &o)
				rid = o.RunnerID
			}
			if dc.subscribedTo != "" && dc.subscribedTo != rid {
				continue
			}
		}
		select {
		case dc.send <- raw:
		default:
			// drop if dashboard is slow
		}
	}
}

func (s *Server) broadcastRunnerList() {
	msg, _ := json.Marshal(s.runnerListMessage())
	s.mu.RLock()
	defer s.mu.RUnlock()
	for dc := range s.dash {
		select {
		case dc.send <- msg:
		default:
		}
	}
}

func (s *Server) runnerListMessage() protocol.RunnerListMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]protocol.RunnerInfo, 0, len(s.runners))
	for _, r := range s.runners {
		list = append(list, r.info)
	}
	return protocol.RunnerListMessage{Type: protocol.TypeRunnerList, Runners: list}
}

func (s *Server) removeRunner(id string) {
	s.mu.Lock()
	delete(s.runners, id)
	s.mu.Unlock()
	s.broadcastRunnerList()
}

func (s *Server) authToken(r *http.Request) bool {
	tok := r.Header.Get("Authorization")
	if len(tok) > 7 && tok[:7] == "Bearer " {
		return tok[7:] == s.cfg.Secret
	}
	return r.URL.Query().Get("token") == s.cfg.Secret
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (s *runnerSession) appendOutput(chunk string) {
	s.partial += chunk
	for {
		i := stringsIndexByte(s.partial, '\n')
		if i < 0 {
			return
		}
		line := s.partial[:i]
		s.partial = s.partial[i+1:]
		s.lines = append(s.lines, line)
		if len(s.lines) > s.lineCap {
			s.lines = s.lines[1:]
		}
	}
}

func stringsIndexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
