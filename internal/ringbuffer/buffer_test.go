package ringbuffer

import "testing"

func TestLines_capacity(t *testing.T) {
	rb := New(3)
	var carry string
	rb.Append("a\nb\nc\nd\n", &carry)
	lines := rb.Snapshot()
	if len(lines) != 3 || lines[0] != "b" || lines[2] != "d" {
		t.Fatalf("unexpected snapshot: %v", lines)
	}
}
