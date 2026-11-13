package app

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/gasmod/gas"
	"github.com/gasmod/gas/config"
)

func loadEnvConfig(t *testing.T, id, secret string) (*Config, error) {
	t.Helper()
	t.Setenv("TULE_GasEnv", "testing")
	t.Setenv("TULE_Auth_AccessKeyID", id)
	t.Setenv("TULE_Auth_SecretAccessKey", secret)

	return LoadConfigFrom(config.NewEnvProvider(
		config.WithEnvSeparator(envVarSeparator),
		config.WithEnvPrefix(envVarPrefix),
	))
}

func TestLoadConfigRequiresCredentials(t *testing.T) {
	tests := []struct {
		name, id, secret string
		wantErr          bool
	}{
		{name: "both set", id: "AKID", secret: "secret"},
		{name: "neither set", wantErr: true},
		{name: "no secret", id: "AKID", wantErr: true},
		{name: "no access key", secret: "secret", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadEnvConfig(t, tt.id, tt.secret)
			if tt.wantErr != errors.Is(err, errNoCredentials) {
				t.Fatalf("got %v, want errNoCredentials: %v", err, tt.wantErr)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadEnvConfig(t, "AKID", "secret")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	// Queue URLs and ARNs depend on these, so a default that drifts
	// silently would change what clients see.
	tests := []struct {
		name, got, want string
	}{
		{"store path", cfg.Store.Path, "./data/queues.db"},
		{"account", cfg.Queues.AccountID, "000000000000"},
		{"region", cfg.Queues.Region, "us-east-1"},
		{"base URL", cfg.Queues.BaseURL, ""},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("default %s = %q, want %q", tt.name, tt.got, tt.want)
		}
	}
	if got := cfg.store.Get("Server.Port"); got != 9050 {
		t.Fatalf("default port = %v, want 9050", got)
	}
}

func TestLoadConfigChecksTheQueueSettings(t *testing.T) {
	tests := []struct {
		name, key, value string
	}{
		{name: "short account ID", key: "TULE_Queues_AccountID", value: "12345"},
		{name: "account ID with letters", key: "TULE_Queues_AccountID", value: "12345678901a"},
		{name: "region", key: "TULE_Queues_Region", value: "Earth"},
		{name: "base URL without a scheme", key: "TULE_Queues_BaseURL", value: "localhost:9050"},
		{name: "base URL with a path", key: "TULE_Queues_BaseURL", value: "http://localhost/sqs"},
		{name: "base URL with a query", key: "TULE_Queues_BaseURL", value: "http://localhost/?a=b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.key, tt.value)
			if _, err := loadEnvConfig(t, "AKID", "secret"); err == nil {
				t.Fatalf("load config with %s=%q succeeded", tt.key, tt.value)
			}
		})
	}

	t.Setenv("TULE_Queues_BaseURL", "https://queues.example.com/")
	t.Setenv("TULE_Queues_Region", "eu-west-2")
	t.Setenv("TULE_Queues_AccountID", "123456789012")
	if _, err := loadEnvConfig(t, "AKID", "secret"); err != nil {
		t.Fatalf("load config with valid queue settings: %v", err)
	}
}

func TestLoadConfigSetsServerTimeouts(t *testing.T) {
	cfg, err := loadEnvConfig(t, "AKID", "secret")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	// Bound through gas's own Config, so a key this service spells wrong
	// fails here rather than silently leaving the gas default in place.
	gasCfg := gas.DefaultConfig()
	if bErr := cfg.store.Bind(gasCfg); bErr != nil {
		t.Fatalf("bind gas config: %v", bErr)
	}

	tests := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{name: "read", got: gasCfg.Server.ReadTimeout, want: 30 * time.Second},
		{name: "read header", got: gasCfg.Server.ReadHeaderTimeout, want: 30 * time.Second},
		{name: "write", got: gasCfg.Server.WriteTimeout, want: 30 * time.Second},
		{name: "idle", got: gasCfg.Server.IdleTimeout, want: 2 * time.Minute},
		{name: "shutdown", got: gasCfg.Server.ShutdownTimeout, want: 7 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s timeout = %s, want %s", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestLoadConfigReadsTheEnvironment(t *testing.T) {
	cfg, err := loadEnvConfig(t, "AKID", "secret")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	// TULE_GasEnv picks the logger, and gas binds its own Config.Env from the
	// same key, so both must see it.
	if got := cfg.Env; got != gas.EnvTesting {
		t.Fatalf("env = %q, want %q", got, gas.EnvTesting)
	}

	gasCfg := gas.DefaultConfig()
	if bErr := cfg.store.Bind(gasCfg); bErr != nil {
		t.Fatalf("bind gas config: %v", bErr)
	}
	if got := gasCfg.Env; got != gas.EnvTesting {
		t.Fatalf("gas env = %q, want %q", got, gas.EnvTesting)
	}
}

func TestZeroReadTimeoutStarts(t *testing.T) {
	// 0 is net/http's "no limit" for a whole request. Gas still requires a
	// positive header timeout, which the default provides, so the app starts.
	t.Setenv("TULE_Server_ReadTimeout", "0")
	t.Setenv("TULE_Store_Path", filepath.Join(t.TempDir(), "queues.db"))

	cfg, err := loadEnvConfig(t, "AKID", "secret")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	a, err := New(cfg)
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if sErr := a.Start(context.Background()); sErr != nil {
		t.Fatalf("start app: %v", sErr)
	}
	t.Cleanup(func() {
		if sErr := a.Shutdown(context.Background()); sErr != nil {
			t.Errorf("shutdown: %v", sErr)
		}
	})

	if got := a.Config().Server.ReadTimeout; got != 0 {
		t.Fatalf("read timeout = %s, want 0", got)
	}
}

func TestLoadConfigLeavesTheIdentityEndpointUnset(t *testing.T) {
	// Unset, queues runs standalone on the root key alone.
	cfg, err := loadEnvConfig(t, "AKID", "secret")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Auth.IdentityEndpoint != "" {
		t.Fatalf("default identity endpoint = %q, want empty", cfg.Auth.IdentityEndpoint)
	}
}

func TestLoadConfigReadsTheIdentityEndpoint(t *testing.T) {
	t.Setenv("TULE_Auth_IdentityEndpoint", "http://127.0.0.1:9030")
	cfg, err := loadEnvConfig(t, "AKID", "secret")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Auth.IdentityEndpoint != "http://127.0.0.1:9030" {
		t.Fatalf("identity endpoint = %q", cfg.Auth.IdentityEndpoint)
	}
}

func TestLoadConfigChecksTheIdentityEndpoint(t *testing.T) {
	for _, value := range []string{"localhost:9030", "http://", "ftp://localhost:9030", "/identity"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("TULE_Auth_IdentityEndpoint", value)
			if _, err := loadEnvConfig(t, "AKID", "secret"); err == nil {
				t.Fatalf("load config with identity endpoint %q succeeded", value)
			}
		})
	}
}
