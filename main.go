package wrdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/pflag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
	httpd "wrdb/http"
	"wrdb/wrdbserver"
)

const (
	DefaultHTTPAddr = "localhost:8091"
	DefaultRaftAddr = "localhost:8089"
)

// Command line parameters
var httpAddr string
var raftAddr string
var joinAddr string
var nodeID string

func init() {
	pflag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	pflag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")
	pflag.StringVar(&joinAddr, "join", "", "Set join address, if any")
	pflag.StringVar(&nodeID, "id", "", "Node ID")
	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
	}
}
func main() {
	pflag.Parse()

	if pflag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}

	// Ensure Raft storage exists.
	raftDir := pflag.Arg(0)
	if raftDir == "" {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll(raftDir, 0700)

	s := wrdbserver.New()
	s.RaftDir = raftDir
	s.RaftBind = raftAddr
	if err := s.Open(joinAddr == "", nodeID); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	// If join was specified, make the join request.
	if joinAddr != "" {
		if err := join(joinAddr, httpAddr, raftAddr, nodeID); err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		}
	} else {
		log.Println("no join addresses set")
	}

	// Wait until the store is in full consensus.
	openTimeout := 120 * time.Second
	s.WaitForLeader(openTimeout)
	s.WaitForApplied(openTimeout)

	// This may be a standalone server. In that case set its own metadata.
	if err := s.SetMeta(nodeID, httpAddr); err != nil && err != wrdbserver.ErrNotLeader {
		// Non-leader errors are OK, since metadata will then be set through
		// consensus as a result of a join. All other errors indicate a problem.
		log.Fatalf("failed to SetMeta at %s: %s", nodeID, err.Error())
	}

	h := httpd.New(httpAddr, s)
	if err := h.Start(); err != nil {
		log.Fatalf("failed to start HTTP service: %s", err.Error())
	}

	log.Println("raftdb started successfully")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("raftdb exiting")
}

func join(joinAddr, httpAddr, raftAddr, nodeID string) error {
	b, err := json.Marshal(map[string]string{"httpAddr": httpAddr, "raftAddr": raftAddr, "id": nodeID})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
