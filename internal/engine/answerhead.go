package engine

import "bytes"

// HeaderBlock is the header lines of the response built so far, "Name: value\r\n" each, without the
// status line. It is a view of the pooled head: a caller that keeps anything from it copies it
// before Serve returns (ADR 0008). It is for a middleware that stores an answer to send again
// (ADR 0090), and it holds what the framework wrote, not the Date, Content-Length and Connection
// the engine adds when it writes the answer.
func (r *Response) HeaderBlock() []byte {
	i := bytes.Index(r.head, []byte("\r\n"))
	if i < 0 {
		return nil
	}
	return r.head[i+2:]
}
