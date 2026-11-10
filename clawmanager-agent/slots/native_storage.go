package slots

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/iamlovingit/clawmanager-agent/gateway"
	"log"
	"os"
	"strings"
)

func (s sandboxStarter) PrepareNativeStorage(ctx context.Context, req gateway.CreateGatewayRequest, port int) error {
	if s.cfg.SandboxProfile != "nfs-rootless-v1" {
		return fmt.Errorf("credential-scoped storage profile required")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(gateway.NativeStorageRequest{Config: gateway.NativeConfig(s.cfg), Request: req, Port: port})
	if err != nil {
		return err
	}
	spec := gateway.GatewayStartSpec{GatewayID: fmt.Sprintf("gw-%d-%d", req.InstanceID, req.Generation), RuntimeType: s.cfg.RuntimeType, InstanceID: req.InstanceID, UserID: req.UserID, WorkspacePath: req.WorkspacePath, Port: port, UID: req.UID, GID: req.GID, Generation: req.Generation, Command: []string{executable, "managed-native-storage"}, Stdin: payload, Env: s.cfg.Runtime.GatewayEnv(s.cfg.GatewayBaseEnv, s.cfg, req, req.WorkspacePath, port)}
	output, err := s.RunGatewayCommand(ctx, spec)
	if err != nil {
		// Helper output may contain persisted configuration. Only fixed diagnostic
		// categories reach logs; never log the payload, environment or raw output.
		log.Printf("runtime-agent native storage preparation failed: instance_id=%d generation=%d category=%s", req.InstanceID, req.Generation, nativeStorageFailureCategory(output, err))
	}
	return err
}

func nativeStorageFailureCategory(output []byte, err error) string {
	text := strings.ToLower(string(output) + "\n" + err.Error())
	phase := ""
	for _, name := range []string{"credentials", "workspace", "config"} {
		if strings.Contains(text, "native_storage_"+name+":") {
			phase = name + "_"
			break
		}
	}
	for _, entry := range []struct{ needle, code string }{
		{"native helper identity mismatch", "helper_identity"},
		{"unsafe managed", "unsafe_managed_configuration"},
		{"foreign", "untrusted_inode"},
		{"bwrap:", "bubblewrap"},
		{"seccomp", "seccomp"},
		{"permission denied", "permission_denied"},
		{"operation not permitted", "operation_not_permitted"},
		{"no such file", "missing_file"},
		{"not found", "missing_command"},
		{"identity", "execution_identity"},
		{"credential", "credential_configuration"},
		{"unsafe", "unsafe_configuration"},
		{"network", "network_preparation"},
		{"context canceled", "cancelled"},
	} {
		if strings.Contains(text, entry.needle) {
			return phase + entry.code
		}
	}
	return phase + "helper_failed"
}
