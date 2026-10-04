package client

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
)

func TestClientRESPRoundTrip(t *testing.T) {
	clientConn, peerConn := net.Pipe()
	defer peerConn.Close()

	client := NewClient(clientConn)
	defer client.Close()

	peerDecoder := resp.NewDecoder(bufio.NewReader(peerConn))
	peerWriter := bufio.NewWriter(peerConn)
	peerEncoder := resp.NewEncoder(peerWriter)

	peerWrite := make(chan error, 1)
	go func() {
		err := peerEncoder.Encode(resp.NewArray(resp.NewBulkString("PING")))
		if err == nil {
			err = peerWriter.Flush()
		}
		peerWrite <- err
	}()

	request, err := client.Decoder().Decode()
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if request.Type != resp.Array || len(request.Array) != 1 || request.Array[0].String != "PING" {
		t.Fatalf("decoded request = %+v, want PING array", request)
	}
	if err := <-peerWrite; err != nil {
		t.Fatalf("write request: %v", err)
	}

	if err := client.Encoder().Encode(resp.NewSimpleString("PONG")); err != nil {
		t.Fatalf("encode response: %v", err)
	}
	flushDone := make(chan error, 1)
	go func() { flushDone <- client.Flush() }()

	response, err := peerDecoder.Decode()
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Type != resp.SimpleString || response.String != "PONG" {
		t.Fatalf("decoded response = %+v, want PONG", response)
	}
	if err := <-flushDone; err != nil {
		t.Fatalf("flush response: %v", err)
	}
	if client.RemoteAddr() == nil {
		t.Fatal("RemoteAddr() is nil for an active connection")
	}
}

func TestClientReadLimitAndDeadlines(t *testing.T) {
	clientConn, peerConn := net.Pipe()
	defer peerConn.Close()
	client := NewClientWithLimit(clientConn, 8)
	defer client.Close()

	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if err := client.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := peerConn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
		writeDone <- err
	}()
	if _, err := client.Decoder().Decode(); err == nil {
		t.Fatal("Decode() accepted a request larger than the configured limit")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close client after rejected request: %v", err)
	}
	<-writeDone
}
