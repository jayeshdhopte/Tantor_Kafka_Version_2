package readiness

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"io.translab/tantor-agent/internal/taskvalidate"
)

type PeerProbe struct {
	HostID   string `json:"hostId"`
	SourceIP string `json:"sourceIp"`
	Roles    string `json:"roles"`
	Token    string `json:"token"`
}
type ProbeRequest struct {
	HostID     string    `json:"hostId"`
	TargetIP   string    `json:"targetIp"`
	TargetPort string    `json:"targetPort"`
	Token      string    `json:"token"`
	ExpiresAt  time.Time `json:"expiresAt"`
}
type probeSession struct {
	listener net.Listener
	mu       sync.Mutex
	received map[string]bool
	expected map[string]PeerProbe
}

// This listener proves a route only. It never impersonates a Kafka protocol endpoint.
func listenProbe(ctx context.Context, address string, probes []PeerProbe) (*probeSession, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	s := &probeSession{listener: listener, received: map[string]bool{}, expected: map[string]PeerProbe{}}
	for _, probe := range probes {
		s.expected[probe.Token] = probe
	}
	go func() { <-ctx.Done(); _ = listener.Close() }()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Bounded reads and no disk writes. Sequential handling limits resource use.
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			_, _ = conn.Write([]byte("TANTOR-READINESS/1\n"))
			var message struct {
				Token string `json:"token"`
			}
			if json.NewDecoder(io.LimitReader(conn, 256)).Decode(&message) == nil {
				s.mu.Lock()
				expected, ok := s.expected[message.Token]
				remote, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
				ok = ok && net.ParseIP(remote).Equal(net.ParseIP(expected.SourceIP))
				if ok {
					s.received[message.Token] = true
				}
				s.mu.Unlock()
				if ok {
					_, _ = conn.Write([]byte("READY\n"))
				}
			}
			_ = conn.Close()
		}
	}()
	return s, nil
}
func (s *probeSession) count(role string) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for token := range s.received {
		for _, r := range strings.Split(s.expected[token].Roles, ",") {
			if role == r {
				n++
				break
			}
		}
	}
	return n
}
func (s *probeSession) wait(ctx context.Context) {
	if s == nil || len(s.expected) == 0 {
		return
	}
	timeout := time.NewTimer(20 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		done := len(s.received) == len(s.expected)
		s.mu.Unlock()
		if done {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-timeout.C:
			return
		case <-ticker.C:
		}
	}
}

func Probe(ctx context.Context, raw, hostID string) error {
	var r ProbeRequest
	if json.Unmarshal([]byte(raw), &r) != nil || r.HostID != hostID || net.ParseIP(r.TargetIP) == nil || len(r.Token) != 36 || !r.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("invalid or expired peer probe")
	}
	if _, err := taskvalidate.Port(r.TargetPort); err != nil {
		return err
	}
	end := r.ExpiresAt
	if limit := time.Now().Add(60 * time.Second); end.After(limit) {
		end = limit
	}
	ctx, cancel := context.WithDeadline(ctx, end)
	defer cancel()
	for {
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", net.JoinHostPort(r.TargetIP, r.TargetPort))
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			var greeting [19]byte
			_, readErr := io.ReadFull(conn, greeting[:])
			if readErr != nil || string(greeting[:]) != "TANTOR-READINESS/1\n" {
				_ = conn.Close()
				select {
				case <-ctx.Done():
					return fmt.Errorf("candidate probe endpoint unavailable")
				case <-time.After(500 * time.Millisecond):
				}
				continue
			}
			if json.NewEncoder(conn).Encode(map[string]string{"token": r.Token}) == nil {
				var response [6]byte
				n, _ := io.ReadFull(conn, response[:])
				_ = conn.Close()
				if string(response[:n]) == "READY\n" {
					return nil
				}
			} else {
				_ = conn.Close()
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("candidate route probe did not complete")
		case <-time.After(500 * time.Millisecond):
		}
	}
}
