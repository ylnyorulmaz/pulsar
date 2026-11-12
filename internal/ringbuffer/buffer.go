package ringbuffer

import "sync"

const defaultCapacity = 500

// Lines keeps the last N complete lines for catch-up on reconnect.
type Lines struct {
	mu       sync.RWMutex
	capacity int
	lines    []string
}

// New creates a ring buffer sized for maxLines (defaults to 500 when <= 0).
func New(maxLines int) *Lines {
	if maxLines <= 0 {
		maxLines = defaultCapacity
	}
	return &Lines{capacity: maxLines, lines: make([]string, 0, maxLines)}
}

// Append ingests a chunk that may contain partial lines; completed lines are stored.
func (l *Lines) Append(chunk string, carry *string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	*carry += chunk
	for {
		i := indexByte(*carry, '\n')
		if i < 0 {
			return
		}
		line := (*carry)[:i]
		*carry = (*carry)[i+1:]
		l.push(line)
	}
}

// Flush stores any trailing partial line (e.g. on shutdown).
func (l *Lines) Flush(carry string) {
	if carry == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.push(carry)
}

func (l *Lines) push(line string) {
	if len(l.lines) >= l.capacity {
		copy(l.lines, l.lines[1:])
		l.lines[len(l.lines)-1] = line
		return
	}
	l.lines = append(l.lines, line)
}

// Snapshot returns a copy of stored lines.
func (l *Lines) Snapshot() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]string, len(l.lines))
	copy(out, l.lines)
	return out
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
