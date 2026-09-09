package hermes

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/iamlovingit/clawmanager-agent/internal/gateway"
)

const desktopWebFlag = "CLAWMANAGER_HERMES_DESKTOP_WEB_ENABLED"

// This is a deployment switch, never a create-request or renderer capability.
func desktopWebEnabled() bool { return os.Getenv(desktopWebFlag) == "true" }

func validateDesktopWebRequest(cfg gateway.Config, req gateway.CreateGatewayRequest) error {
	mode := strings.TrimSpace(os.Getenv("CLAWMANAGER_HERMES_BACKEND_MODE"))
	if mode != "" && mode != "dashboard" {
		return fmt.Errorf("unsupported_hermes_protocol: only dashboard mode is verified")
	}
	if enabled, _ := requestEnvValue(req, "CLAWMANAGER_TEAM_ENABLED"); truthy(enabled) {
		return fmt.Errorf("unsupported_hermes_protocol: Team Desktop Web is not enabled")
	}
	if team, _ := requestEnvValue(req, "CLAWMANAGER_TEAM_CONFIG_JSON"); strings.TrimSpace(team) != "" {
		return fmt.Errorf("unsupported_hermes_protocol: Team Desktop Web is not enabled")
	}
	if req.InstanceID <= 0 || req.UserID <= 0 || req.Generation <= 0 || req.UID <= 0 || req.GID <= 0 {
		return fmt.Errorf("invalid_gateway_identity: positive instance, user, generation, uid and gid are required")
	}
	// Linux credentials are uint32; reject truncation to root and the special
	// all-ones identity before chown or process creation.
	if uint64(req.UID) >= 1<<32-1 || uint64(req.GID) >= 1<<32-1 {
		return fmt.Errorf("invalid_gateway_identity: uid and gid exceed the supported range")
	}
	if cfg.GatewayPortBlockSize > 1 {
		return fmt.Errorf("port_conflict: Hermes Desktop Web requires one assigned port")
	}
	if req.GatewayPort != 0 && (req.GatewayPort < cfg.GatewayPortStart || req.GatewayPort > cfg.GatewayPortEnd) {
		return fmt.Errorf("port_conflict: assigned port is outside the configured pool")
	}
	if req.PortRange.Start != 0 || req.PortRange.End != 0 {
		if req.PortRange.Start < cfg.GatewayPortStart || req.PortRange.End > cfg.GatewayPortEnd || req.PortRange.End < req.PortRange.Start {
			return fmt.Errorf("port_conflict: requested range is outside the configured pool")
		}
	}
	_, _, err := dashboardCredentials(req)
	return err
}

func dashboardCredentials(req gateway.CreateGatewayRequest) (string, string, error) {
	username, _ := requestEnvValue(req, "HERMES_DASHBOARD_BASIC_AUTH_USERNAME")
	if username == "" {
		username = "clawmanager"
	}
	password, _ := requestEnvValue(req, "HERMES_DASHBOARD_BASIC_AUTH_PASSWORD", "CLAWMANAGER_DASHBOARD_BASIC_AUTH_PASSWORD", "CLAWMANAGER_INSTANCE_ACCESS_TOKEN", "CLAWMANAGER_INSTANCE_TOKEN")
	if strings.TrimSpace(password) == "" || strings.ContainsAny(username+password, "\r\n\x00") || strings.Contains(username, ":") {
		return "", "", fmt.Errorf("dashboard_auth_failed: managed instance credentials are required")
	}
	return username, password, nil
}

func desktopWebEnvironment(base []string, cfg gateway.Config, req gateway.CreateGatewayRequest, workspace string, port int) []string {
	var env []string
	// Pod credentials, Python/Node injection switches, global HOME and provider
	// keys are deliberately absent. The image owns PATH; requests cannot set it.
	for _, key := range []string{"PATH", "LANG", "LC_ALL", "TZ", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if value := envValue(base, key); value != "" {
			env = setEnv(env, key, value)
		}
	}
	for _, key := range []string{"CLAWMANAGER_LLM_MODEL", "CLAWMANAGER_LLM_PROVIDER", "OPENAI_MODEL", "CLAWMANAGER_INSTANCE_TOKEN"} {
		if value, ok := requestEnvValue(req, key); ok {
			env = setEnv(env, key, value)
		}
	}
	if resolved, err := desktopWebLLMConfig(cfg, req); err == nil {
		for _, key := range []string{"CLAWMANAGER_LLM_API_KEY", "OPENAI_API_KEY"} {
			env = setEnv(env, key, resolved.LLMAPIKey)
		}
		for _, key := range []string{"CLAWMANAGER_LLM_BASE_URL", "OPENAI_BASE_URL", "OPENAI_API_BASE", "CUSTOM_BASE_URL"} {
			env = setEnv(env, key, resolved.LLMBaseURL)
		}
	}
	if username, password, err := dashboardCredentials(req); err == nil {
		env = setEnv(env, "HERMES_DASHBOARD_BASIC_AUTH_USERNAME", username)
		env = setEnv(env, "HERMES_DASHBOARD_BASIC_AUTH_PASSWORD", password)
	}
	if origin, proxies, err := desktopWebProxyConfig(cfg, req); err == nil {
		env = setEnv(env, "CLAWMANAGER_CONTROL_UI_ORIGIN", origin)
		env = setEnv(env, "CLAWMANAGER_TRUSTED_PROXY_CIDRS", strings.Join(proxies, ","))
	}
	values := map[string]string{
		desktopWebFlag: "true", "CLAWMANAGER_HERMES_BACKEND_MODE": "dashboard",
		"CLAWMANAGER_INSTANCE_ID": strconv.Itoa(req.InstanceID), "CLAWMANAGER_GATEWAY_GENERATION": strconv.Itoa(req.Generation),
		"CLAWMANAGER_USER_ID": strconv.Itoa(req.UserID), "CLAWMANAGER_RUNTIME_TYPE": "hermes",
		"CLAWMANAGER_WORKSPACE_PATH": workspace, "CLAWMANAGER_GATEWAY_PORT": strconv.Itoa(port),
		"HOME": filepath.Join(workspace, "home"), "HERMES_HOME": filepath.Join(workspace, "home", ".hermes"),
		"HOST": "0.0.0.0", "PORT": strconv.Itoa(port), "HERMES_ACCEPT_HOOKS": "0",
		"XDG_CACHE_HOME":  filepath.Join(workspace, "home", ".cache"),
		"XDG_CONFIG_HOME": filepath.Join(workspace, "home", ".config"),
		"XDG_DATA_HOME":   filepath.Join(workspace, "home", ".local", "share"),
	}
	for key, value := range values {
		env = setEnv(env, key, value)
	}
	return env
}

func prepareDesktopWebWorkspace(cfg gateway.Config, req gateway.CreateGatewayRequest, workspace string) error {
	for _, part := range strings.FieldsFunc(req.WorkspacePath, func(r rune) bool { return r == '/' || r == '\\' }) {
		if part == ".." {
			return gateway.ErrWorkspacePath
		}
	}
	expected, err := gateway.ValidateWorkspacePath(cfg.WorkspaceRoot, cfg.RuntimeType, req)
	if err != nil || expected != workspace {
		return gateway.ErrWorkspacePath
	}
	// Use the writer's directory-handle walk so preparation applies the same
	// identity check before ownership changes, including on older workspaces.
	root, err := openDesktopWebHome(cfg, req, workspace)
	if err != nil {
		return err
	}
	return root.Close()
}
