package redact

import "testing"

func TestBytes_masksCommonSecrets(t *testing.T) {
	in := []byte("key=sk-abcdefghijklmnopqrstuvwxyz and ghp_abcdefghijklmnopqrstuvwxyz")
	out := string(Bytes(in))
	if out == string(in) {
		t.Fatal("expected redaction")
	}
	if !contains(out, mask) {
		t.Fatalf("expected %q in %q", mask, out)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || index(s, sub) >= 0)
}

func index(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
