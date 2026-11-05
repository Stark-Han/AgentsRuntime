package deepseek

import (
	"github.com/iamlovingit/clawmanager-agent/gateway"
	"github.com/iamlovingit/clawmanager-agent/runtime/generic"
	"strings"
)

// Only the trusted slot lifecycle provides an instance-private outer /tmp.
// Legacy process pools must retain native DSH's isolated tmpfs behavior.
func (p Profile) GatewayEnv(base []string, cfg gateway.Config, req gateway.CreateGatewayRequest, workspace string, port int) []string {
	env := p.Profile.GatewayEnv(base, cfg, req, workspace, port)
	filtered := env[:0]
	for _, entry := range env {
		if !strings.HasPrefix(entry, "CLAWMANAGER_DSH_PRIVATE_TMP=") {
			filtered = append(filtered, entry)
		}
	}
	value := "0"
	if cfg.IsolatedLifecycle {
		value = "1"
	}
	return append(filtered, "CLAWMANAGER_DSH_PRIVATE_TMP="+value)
}

type Profile struct {
	generic.Profile
}

func NewProfile(runtimeType string) Profile {
	return Profile{Profile: generic.NewProfile(runtimeType)}
}

func (Profile) DisplayName() string {
	return "DeepSeek Harness"
}

func (Profile) HealthChecker(cfg gateway.Config) gateway.GatewayHealthChecker {
	return gateway.NewHTTPGatewayHealthChecker(cfg)
}
