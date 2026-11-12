package redact

import "regexp"

var patterns = []*regexp.Regexp{
	regexp.MustCompile(`sk-[a-zA-Z0-9]{20,}`),
	regexp.MustCompile(`ghp_[a-zA-Z0-9]{20,}`),
	regexp.MustCompile(`AKIA[0-9A-Z]{16}`),
	regexp.MustCompile(`xox[baprs]-[a-zA-Z0-9\-]{10,}`),
}

const mask = "***REDACTED***"

// Bytes applies lightweight secret masking before network send.
func Bytes(b []byte) []byte {
	s := string(b)
	for _, re := range patterns {
		s = re.ReplaceAllString(s, mask)
	}
	return []byte(s)
}
