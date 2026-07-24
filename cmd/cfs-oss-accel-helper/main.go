// cfs-oss-accel-helper: standalone CLI that performs one oss-accel cold-read
// recall (calls a mover's /ossAccelRecall) and reports the outcome purely via
// its exit code. Exists so the kernel client's cold-read gate — which has no
// existing mechanism to make an HTTP call from kernel space — can shell out
// to a small, independently-testable userspace helper via call_usermodehelper
// instead of embedding an HTTP client or S3 stack in kernel code.
//
// This deliberately mirrors client/fs/oss_accel.go's ossAccelColdReadGate HTTP
// logic (same URL shape, same status-code interpretation) rather than sharing
// code with it — that gate is tied into the FUSE client's cache-invalidation
// path, whereas this helper only needs to make one HTTP call and exit.
//
// Exit codes (the kernel caller reads only this, not stdout):
//
//	0  success (mover returned 200 — the inode is now StorageClass=Replica)
//	11 not yet safe to recall (mover returned 425 — caller should surface EAGAIN)
//	1  any other error (bad args, network failure, non-200/425 status)
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	exitSuccess    = 0
	exitTryAgain   = 11
	exitOtherError = 1
)

// recallHTTPTimeout matches the FUSE client cold-read gate's own timeout
// (client/fs/oss_accel.go: ossAccelHTTPClient) — the mover's recall response
// only arrives once its S3-GET-and-write loop (or an early 425) completes.
const recallHTTPTimeout = 5 * time.Minute

func main() {
	os.Exit(run())
}

func run() int {
	var mover, vol, path, checksum, token string
	var ino, size uint64
	var sc, vsc, asc uint

	flag.StringVar(&mover, "mover", "", "mover (lcnode) address, host:port")
	flag.StringVar(&vol, "vol", "", "volume name")
	flag.Uint64Var(&ino, "ino", 0, "inode number")
	flag.Uint64Var(&size, "size", 0, "file size in bytes")
	flag.UintVar(&sc, "sc", 0, "volume's default storage class")
	flag.UintVar(&vsc, "vsc", 0, "volume's replica storage class (recall target)")
	flag.UintVar(&asc, "asc", 0, "volume's allowed storage classes (single value, repeated as needed)")
	flag.StringVar(&path, "path", "", "file path within the volume (also the S3 key basis)")
	flag.StringVar(&checksum, "checksum", "", "expected sha256, e.g. sha256:<hex> or bare hex")
	// 系统层面收尾: lcnode's /ossAccelRecall is now gated behind the shared
	// admin token (lcnode/oss_accel_auth.go) — passed through from the
	// kernel module's mount option, same value configured on lcnode's own
	// side. Empty (default) sends no Authorization header.
	flag.StringVar(&token, "token", "", "shared admin token for lcnode's oss-accel endpoints")
	flag.Parse()

	if mover == "" || vol == "" || ino == 0 || path == "" {
		fmt.Fprintln(os.Stderr, "cfs-oss-accel-helper: missing required flag (need at least -mover -vol -ino -path)")
		return exitOtherError
	}

	url := fmt.Sprintf("http://%s/ossAccelRecall?vol=%s&ino=%d&size=%d&sc=%d&vsc=%d&asc=%d&path=%s&checksum=%s",
		mover, vol, ino, size, sc, vsc, asc, path, checksum)

	req, rerr := http.NewRequest(http.MethodGet, url, nil)
	if rerr != nil {
		fmt.Fprintf(os.Stderr, "cfs-oss-accel-helper: build request err: %v\n", rerr)
		return exitOtherError
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := &http.Client{Timeout: recallHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cfs-oss-accel-helper: mover request err: %v\n", err)
		return exitOtherError
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK:
		fmt.Fprintf(os.Stdout, "%s\n", body)
		return exitSuccess
	case http.StatusTooEarly:
		fmt.Fprintf(os.Stderr, "cfs-oss-accel-helper: not yet safe to recall: %s\n", body)
		return exitTryAgain
	default:
		fmt.Fprintf(os.Stderr, "cfs-oss-accel-helper: recall failed, status(%d): %s\n", resp.StatusCode, body)
		return exitOtherError
	}
}
