package agent

import (
	"strings"
	"testing"
)

func TestHermesLiteRequiresInternalBackendAndImmutableImage(t *testing.T) {
	t.Setenv("CLAWMANAGER_RUNTIME_TYPE", "hermes")
	t.Setenv("CLAWMANAGER_HERMES_DESKTOP_WEB_ENABLED", "true")
	t.Setenv("RUNTIME_AGENT_CONTROL_TOKEN", "control")
	t.Setenv("RUNTIME_AGENT_REPORT_TOKEN", "report")
	t.Setenv("CLAWMANAGER_BACKEND_URL", "http://clawmanager-gateway.platform.svc:9001")
	t.Setenv("CLAWMANAGER_CONTROL_UI_ORIGIN", "")
	t.Setenv("CLAWMANAGER_TRUSTED_PROXY_CIDRS", "")
	t.Setenv("CLAWMANAGER_RUNTIME_IMAGE_REF", "example/hermes@sha256:"+strings.Repeat("a", 64))
	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PublicOrigin != "" || len(cfg.TrustedProxies) != 0 {
		t.Fatal("inferred proxy trust in Lite mode")
	}
	for _, image := range []string{"example/hermes:latest", "example/hermes@sha256:" + strings.Repeat("z", 64), "@sha256:" + strings.Repeat("a", 64)} {
		t.Setenv("CLAWMANAGER_RUNTIME_IMAGE_REF", image)
		if _, err := LoadConfigFromEnv(); err == nil {
			t.Errorf("accepted image %s", image)
		}
	}
	t.Setenv("CLAWMANAGER_RUNTIME_IMAGE_REF", "example/hermes@sha256:"+strings.Repeat("a", 64))
	t.Setenv("CLAWMANAGER_BACKEND_URL", "https://public.example")
	if _, err := LoadConfigFromEnv(); err == nil {
		t.Fatal("external backend accepted")
	}
}
