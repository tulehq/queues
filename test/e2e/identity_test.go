//go:build e2e

package e2e

import (
	"bytes"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// These tests run queues against a real identity service: identity is
// built from its checkout and run as a process with the same root key, and
// queues is pointed at it through TULE_Auth_IdentityEndpoint.

// identityDirEnv names the identity checkout to build, for a layout where it
// is not queues' sibling.
const identityDirEnv = "TULE_E2E_IDENTITY_DIR"

// The root key both services are configured with: the AWS documentation's
// example key, since identity holds an access key ID to at least 16
// characters.
const (
	identityRootKeyID  = "AKIAIOSFODNN7EXAMPLE"
	identityRootSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

type identityStack struct {
	srv       *server
	identity  *identityProcess
	iam       *iam.Client
	rootCreds aws.Credentials
}

func TestIdentityIssuedKeys(t *testing.T) {
	s := startIdentityStack(t)
	userKey := createUserKey(t, s.iam, "dev")

	t.Run("a user key works", func(t *testing.T) {
		c := sqsClientWith(s.srv, userKey)
		u := createQueue(t, c, "from-user", nil)
		send(t, c, u, "hello")
		requireEqual(t, aws.ToString(receiveOne(t, c, u).Body), "hello", "body")
		requireOK(t, inspectorWith(s.srv, userKey).do(t, http.MethodGet, "/_queues/queues", nil),
			"inspection with the user key")
	})
	t.Run("the root key keeps working", func(t *testing.T) {
		// "Every accepted principal shares the queues" (design).
		c := sqsClientWith(s.srv, s.rootCreds)
		requireEqual(t, createQueue(t, c, "from-user", nil), s.srv.queueURL("from-user"),
			"the user's queue seen with the root key")
	})
	t.Run("an inactive key is refused", s.testInactiveKey)
	t.Run("a deleted key is refused", s.testDeletedKey)
}

func startIdentityStack(t *testing.T) *identityStack {
	t.Helper()

	port := identityFreePort(t)
	identityURL := "http://127.0.0.1:" + strconv.Itoa(port)
	s := &identityStack{
		rootCreds: aws.Credentials{
			AccessKeyID:     identityRootKeyID,
			SecretAccessKey: identityRootSecret,
		},
	}
	s.identity = startIdentity(t, port)
	s.srv = startServer(t, map[string]string{
		"TULE_Auth_AccessKeyID":      identityRootKeyID,
		"TULE_Auth_SecretAccessKey":  identityRootSecret,
		"TULE_Auth_IdentityEndpoint": identityURL,
	})
	s.iam = iamClient(identityURL, s.rootCreds)
	return s
}

func (s *identityStack) testInactiveKey(t *testing.T) {
	key := createUserKey(t, s.iam, "inactive")
	_, err := s.iam.UpdateAccessKey(t.Context(), &iam.UpdateAccessKeyInput{
		UserName:    aws.String("inactive"),
		AccessKeyId: aws.String(key.AccessKeyID),
		Status:      iamtypes.StatusTypeInactive,
	})
	requireNoErr(t, err, "deactivate the key")
	s.requireRefused(t, key)
}

func (s *identityStack) testDeletedKey(t *testing.T) {
	key := createUserKey(t, s.iam, "deleted")
	_, err := sqsClientWith(s.srv, key).ListQueues(t.Context(), &sqs.ListQueuesInput{})
	requireNoErr(t, err, "list queues before the key is deleted")

	_, err = s.iam.DeleteAccessKey(t.Context(), &iam.DeleteAccessKeyInput{
		UserName:    aws.String("deleted"),
		AccessKeyId: aws.String(key.AccessKeyID),
	})
	requireNoErr(t, err, "delete the key")
	s.requireRefused(t, key)
}

// requireRefused fails unless both the SQS API and the inspection API
// refuse key with 403 InvalidClientTokenId, the code of a key that "does
// not exist in our records" (Common Errors), and the refused call created
// nothing.
func (s *identityStack) requireRefused(t *testing.T, key aws.Credentials) {
	t.Helper()

	err := errOf(sqsClientWith(s.srv, key).CreateQueue(t.Context(),
		&sqs.CreateQueueInput{QueueName: aws.String("refused")}))
	requireCode(t, err, "InvalidClientTokenId")
	requireStatus(t, err, http.StatusForbidden)
	requireQueueMissing(t, sqsClientWith(s.srv, s.rootCreds), "refused")

	requireAPIErrorBody(t, inspectorWith(s.srv, key).do(t, http.MethodGet, "/_queues/queues", nil),
		http.StatusForbidden, "InvalidClientTokenId", "inspection with a refused key")
}

func iamClient(url string, creds aws.Credentials) *iam.Client {
	return iam.New(iam.Options{
		Region:           region,
		BaseEndpoint:     aws.String(url),
		Credentials:      credentials.StaticCredentialsProvider{Value: creds},
		HTTPClient:       &http.Client{Timeout: clientTimeout},
		RetryMaxAttempts: 1,
	})
}

// createUserKey creates an IAM user and an access key for it.
func createUserKey(t *testing.T, ic *iam.Client, user string) aws.Credentials {
	t.Helper()
	_, err := ic.CreateUser(t.Context(), &iam.CreateUserInput{UserName: aws.String(user)})
	requireNoErr(t, err, "create user "+user)
	out, err := ic.CreateAccessKey(
		t.Context(),
		&iam.CreateAccessKeyInput{UserName: aws.String(user)},
	)
	requireNoErr(t, err, "create access key for "+user)
	return aws.Credentials{
		AccessKeyID:     aws.ToString(out.AccessKey.AccessKeyId),
		SecretAccessKey: aws.ToString(out.AccessKey.SecretAccessKey),
	}
}

// identityProcess is a running service process: identity, or accounts.
type identityProcess struct {
	cmd     *exec.Cmd
	exited  chan error
	out     *identityOutput
	name    string
	stopped bool
}

// identityOutput collects the process's output, safe to write from the
// process's copying goroutines and read from the test.
type identityOutput struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (b *identityOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p) //nolint:wrapcheck // bytes.Buffer.Write never fails
}

func (b *identityOutput) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startIdentity builds identity from its checkout and runs it on port with
// a fresh store, the root key queues has and extra environment, stopped
// when the test ends.
func startIdentity(t *testing.T, port int, extra ...string) *identityProcess {
	t.Helper()
	dir := t.TempDir()
	env := append([]string{
		"TULE_Server_Port=" + strconv.Itoa(port),
		"TULE_Store_Path=" + filepath.Join(dir, "identity.db"),
	}, extra...)
	return startProcess(t, "identity", buildService(t, "identity", identityDirEnv), dir, port, env)
}

// startProcess runs bin in dir as the service name on port, with
// TULE_GasEnv=testing, the root key and env, and waits for it to be ready.
// It is stopped when the test ends.
func startProcess(t *testing.T, name, bin, dir string, port int, env []string) *identityProcess {
	t.Helper()

	cmd := exec.Command(bin) //nolint:gosec // G204: the binary this test just built
	cmd.Dir = dir
	cmd.Env = append(envWithoutTule(),
		"TULE_GasEnv=testing",
		"TULE_Auth_AccessKeyID="+identityRootKeyID,
		"TULE_Auth_SecretAccessKey="+identityRootSecret,
	)
	cmd.Env = append(cmd.Env, env...)
	p := &identityProcess{name: name, cmd: cmd, exited: make(chan error, 1), out: &identityOutput{}}
	cmd.Stdout = p.out
	cmd.Stderr = p.out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	go func() { p.exited <- cmd.Wait() }()
	t.Cleanup(func() { p.Stop(t) })

	p.waitReady(t, "http://127.0.0.1:"+strconv.Itoa(port))
	return p
}

// buildService builds the service name from its checkout into a temporary
// directory. The test runs in queues/test/e2e; by default the checkout is
// queues' sibling, and dirEnv names another. CI sets it. A missing checkout
// fails the test, so a cross-service test is never silently left out.
func buildService(t *testing.T, name, dirEnv string) string {
	t.Helper()

	src, from := os.Getenv(dirEnv), dirEnv
	if src == "" {
		src, from = filepath.Join("..", "..", "..", name), "the default sibling path"
	}
	src, err := filepath.Abs(src)
	requireNoErr(t, err, name+" directory")
	//nolint:gosec // G703: the checkout the test is told to build
	if _, err = os.Stat(filepath.Join(src, "go.mod")); err != nil {
		t.Fatalf("no %s checkout at %s (from %s); set %s to a %s checkout: %v",
			name, src, from, dirEnv, name, err)
	}

	bin := filepath.Join(t.TempDir(), "tule-"+name)
	//nolint:gosec // G204: go build of the checkout into this test's directory
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, "./cmd")
	build.Dir = src
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, out)
	}
	return bin
}

// envWithoutTule is this process's environment minus the TULE_ variables
// the tests set for queues, so the identity process gets only its own.
func envWithoutTule() []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "TULE_") {
			env = append(env, kv)
		}
	}
	return env
}

func identityFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	requireNoErr(t, err, "find a free port")
	port := l.Addr().(*net.TCPAddr).Port
	requireNoErr(t, l.Close(), "release the free port")
	return port
}

// waitReady polls identity's readiness probe until it answers 200.
func (p *identityProcess) waitReady(t *testing.T, url string) {
	t.Helper()

	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(clientTimeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-p.exited:
			t.Fatalf("%s exited before it was ready: %v\n%s", p.name, err, p.out.String())
		default:
		}
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url+"/_ready", nil)
		requireNoErr(t, err, "build readiness request")
		if res, err := client.Do(req); err == nil {
			_ = res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s was not ready within %v\n%s", p.name, clientTimeout, p.out.String())
}

// Stop interrupts the process and kills it if it does not exit in time. It
// is idempotent.
func (p *identityProcess) Stop(t *testing.T) {
	t.Helper()

	if p.stopped {
		return
	}
	p.stopped = true

	_ = p.cmd.Process.Signal(os.Interrupt)
	select {
	case err := <-p.exited:
		if err != nil {
			t.Logf("%s exited: %v", p.name, err)
		}
	case <-time.After(clientTimeout):
		_ = p.cmd.Process.Kill()
		err := <-p.exited
		t.Errorf("%s did not stop on interrupt (%v)\n%s", p.name, err, p.out.String())
	}
}
