package gateway

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/iamlovingit/clawmanager-agent/internal/openclawcompat"
)

var safeUpgradeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type WorkspaceUpgradeRequest struct {
	RolloutID             string `json:"rollout_id"`
	SnapshotID            string `json:"snapshot_id,omitempty"`
	InstanceID            int    `json:"instance_id"`
	UserID                int    `json:"user_id"`
	Generation            int    `json:"generation"`
	LeaseToken            string `json:"lease_token,omitempty"`
	OfficialDBCheck       bool   `json:"official_database_check,omitempty"`
	PreserveSessionSQLite bool   `json:"preserve_session_sqlite,omitempty"`
	UID                   int    `json:"uid,omitempty"`
	GID                   int    `json:"gid,omitempty"`
}

type SessionSQLiteMigrationResult struct {
	InstanceID           int              `json:"instance_id"`
	Status               string           `json:"status"`
	OutputSHA256         string           `json:"output_sha256"`
	ArchiveBytes         int64            `json:"archive_bytes"`
	ArchiveFiles         int64            `json:"archive_files"`
	RollbackAvailable    bool             `json:"rollback_available"`
	ConfigOriginalSHA256 string           `json:"config_original_sha256,omitempty"`
	ConfigTargetSHA256   string           `json:"config_target_sha256,omitempty"`
	StateCapsuleBytes    int64            `json:"state_capsule_bytes,omitempty"`
	SessionCount         int              `json:"session_count,omitempty"`
	SessionCatalogSHA256 string           `json:"session_catalog_sha256,omitempty"`
	PhaseDurationsMS     map[string]int64 `json:"phase_durations_ms,omitempty"`
	CompletedAt          time.Time        `json:"completed_at"`
}

type openClawSessionCatalog struct {
	Sessions []struct {
		Key       string `json:"key"`
		SessionID string `json:"sessionId"`
	} `json:"sessions"`
}

type SessionSQLiteRestoreResult struct {
	InstanceID             int              `json:"instance_id"`
	Status                 string           `json:"status"`
	OutputSHA256           string           `json:"output_sha256"`
	ConfigRestored         bool             `json:"config_restored"`
	StateRestored          bool             `json:"state_restored"`
	PreservedSessionSQLite bool             `json:"preserved_session_sqlite"`
	PhaseDurationsMS       map[string]int64 `json:"phase_durations_ms,omitempty"`
	CompletedAt            time.Time        `json:"completed_at"`
}

type UpgradeCompatibilityResult struct {
	InstanceID           int       `json:"instance_id"`
	Status               string    `json:"status"`
	ConfigOriginalSHA256 string    `json:"config_original_sha256"`
	ConfigTargetSHA256   string    `json:"config_target_sha256"`
	ConfigBytes          int64     `json:"config_bytes"`
	SessionBytes         int64     `json:"session_bytes"`
	StateBytes           int64     `json:"state_bytes"`
	AvailableBytes       uint64    `json:"available_bytes"`
	ConfigValidated      bool      `json:"config_validated"`
	DoctorValidated      bool      `json:"doctor_validated"`
	SessionDryRunValid   bool      `json:"session_dry_run_valid"`
	ProbeOutputSHA256    string    `json:"probe_output_sha256"`
	CheckedAt            time.Time `json:"checked_at"`
}

type upgradeConfigCapsule struct {
	SchemaVersion  int    `json:"schema_version"`
	InstanceID     int    `json:"instance_id"`
	OriginalExists bool   `json:"original_exists"`
	OriginalMode   uint32 `json:"original_mode"`
	OriginalSHA256 string `json:"original_sha256"`
	TargetSHA256   string `json:"target_sha256"`
}

type upgradeStateCapsule struct {
	SchemaVersion  int             `json:"schema_version"`
	InstanceID     int             `json:"instance_id"`
	OriginalExists bool            `json:"original_exists"`
	TotalBytes     int64           `json:"total_bytes"`
	FileCount      int64           `json:"file_count"`
	WorkspaceFiles map[string]bool `json:"workspace_files"`
	ControlDirs    map[string]bool `json:"control_dirs"`
}

const maxUpgradeConfigBytes = 1 << 20

var errUpgradeProbeBusy = errors.New("upgrade probe source is temporarily busy")

// PreflightUpgradeCompatibility rehearses the complete 8.1 repair and session
// dry-run in an isolated HOME. OpenClaw CLI startup is allowed to create and
// migrate SQLite state, so pointing HOME at the live workspace is not a
// read-only preflight. The probe contains only state/session migration inputs,
// plugin metadata and the two workspace files doctor may migrate.
func (m *GatewayManager) PreflightUpgradeCompatibility(ctx context.Context, req WorkspaceUpgradeRequest) (UpgradeCompatibilityResult, error) {
	result := UpgradeCompatibilityResult{InstanceID: req.InstanceID}
	if err := validateUpgradeID("rollout_id", req.RolloutID); err != nil {
		return result, err
	}
	workspace, err := m.workspaceForUpgrade(req)
	if err != nil {
		return result, err
	}
	if !IsOpenClawAtLeast(m.cfg.OpenClawVersion, OpenClaw81Version) {
		return result, fmt.Errorf("OpenClaw %s does not support upgrade compatibility preflight", m.cfg.OpenClawVersion)
	}
	if req.UID <= 0 || req.GID <= 0 {
		return result, errors.New("positive uid and gid are required for upgrade compatibility preflight")
	}
	inventory, err := m.PreflightWorkspace(ctx, req)
	if err != nil {
		return result, err
	}
	configPath := filepath.Join(workspace, "home", ".openclaw", "openclaw.json")
	raw, readErr := os.ReadFile(configPath)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return result, fmt.Errorf("read OpenClaw config: %w", readErr)
	}
	if len(raw) > maxUpgradeConfigBytes {
		return result, fmt.Errorf("OpenClaw config exceeds %d byte upgrade limit", maxUpgradeConfigBytes)
	}
	config := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &config); err != nil {
			return result, fmt.Errorf("parse OpenClaw config: %w", err)
		}
	}
	openclawcompat.Normalize81(config)
	probeRoot, probeHome, stateBytes, err := m.createUpgradeCompatibilityProbe(workspace, config, req)
	if err != nil {
		return result, err
	}
	defer removeUpgradeProbe(probeRoot)
	probeConfigPath := filepath.Join(probeHome, ".openclaw", "openclaw.json")
	probeHash := sha256.New()
	if err := runOpenClawCommandInHome(ctx, probeRoot, probeHome, probeConfigPath, req, "doctor-fix", []string{"doctor", "--fix", "--yes", "--non-interactive"}, probeHash, 10*time.Minute); err != nil {
		if errors.Is(err, errUpgradeProbeBusy) {
			result.Status = "deferred"
			result.CheckedAt = time.Now().UTC()
			return result, nil
		}
		return result, err
	}
	result.DoctorValidated = true
	if err := runOpenClawCommandInHome(ctx, probeRoot, probeHome, probeConfigPath, req, "config-validate", []string{"config", "validate", "--json"}, probeHash, 2*time.Minute); err != nil {
		return result, err
	}
	result.ConfigValidated = true
	if err := runOpenClawCommandInHome(ctx, probeRoot, probeHome, probeConfigPath, req, "dry-run", sessionSQLiteMigrationCommands()[0], probeHash, 10*time.Minute); err != nil {
		if errors.Is(err, errUpgradeProbeBusy) {
			result.Status = "deferred"
			result.CheckedAt = time.Now().UTC()
			return result, nil
		}
		return result, err
	}
	result.SessionDryRunValid = true
	target, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return result, err
	}
	target = append(target, '\n')
	originalHash := sha256.Sum256(raw)
	targetHash := sha256.Sum256(target)
	result.Status = "compatible"
	result.ConfigOriginalSHA256 = hex.EncodeToString(originalHash[:])
	result.ConfigTargetSHA256 = hex.EncodeToString(targetHash[:])
	result.ConfigBytes = int64(len(raw))
	result.SessionBytes = inventory.TotalBytes
	result.StateBytes = stateBytes
	result.AvailableBytes = inventory.AvailableBytes
	result.ProbeOutputSHA256 = hex.EncodeToString(probeHash.Sum(nil))
	result.CheckedAt = time.Now().UTC()
	return result, nil
}

func (m *GatewayManager) createUpgradeCompatibilityProbe(workspace string, sourceConfig map[string]any, req WorkspaceUpgradeRequest) (string, string, int64, error) {
	probeRoot, err := os.MkdirTemp("", fmt.Sprintf("clawmanager-upgrade-%d-", req.InstanceID))
	if err != nil {
		return "", "", 0, err
	}
	ok := false
	defer func() {
		if !ok {
			removeUpgradeProbe(probeRoot)
		}
	}()
	probeHome := filepath.Join(probeRoot, "home")
	probeOpenClaw := filepath.Join(probeHome, ".openclaw")
	for _, path := range []string{probeOpenClaw, filepath.Join(probeRoot, "tmp"), filepath.Join(probeRoot, "cache"), filepath.Join(probeRoot, "xdg-state")} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return "", "", 0, err
		}
	}

	// Clone before remapping: the target hash must describe the real normalized
	// configuration, not the sandbox-only path substitutions.
	rawConfig, err := json.Marshal(sourceConfig)
	if err != nil {
		return "", "", 0, err
	}
	probeConfig := map[string]any{}
	if err := json.Unmarshal(rawConfig, &probeConfig); err != nil {
		return "", "", 0, err
	}
	workspaceCopies := remapProbeWorkspacePaths(probeConfig, probeOpenClaw)
	workspaceCopies[filepath.Join(workspace, "home", ".openclaw", "workspace")] = filepath.Join(probeOpenClaw, "workspace")
	probeConfigRaw, err := json.MarshalIndent(probeConfig, "", "  ")
	if err != nil {
		return "", "", 0, err
	}
	probeConfigRaw = append(probeConfigRaw, '\n')
	if err := atomicWriteFile(filepath.Join(probeOpenClaw, "openclaw.json"), probeConfigRaw, 0o600); err != nil {
		return "", "", 0, err
	}

	sourceOpenClaw := filepath.Join(workspace, "home", ".openclaw")
	var stateBytes int64
	err = filepath.WalkDir(sourceOpenClaw, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		rel, relErr := filepath.Rel(sourceOpenClaw, path)
		if relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return ErrWorkspacePath
		}
		if rel == "." {
			return nil
		}
		slash := filepath.ToSlash(rel)
		root := strings.SplitN(slash, "/", 2)[0]
		isState := root == "state"
		isControl := isState || root == "cron" || root == "tasks" || root == "automation" || root == "automations"
		isSupport := root == "extensions" || root == "npm" || root == "plugin-skills"
		isSession := sessionMigrationDirectory(slash) || sessionMigrationFile(slash)
		wanted := isControl || isSupport || isSession
		if entry.IsDir() {
			if !wanted {
				return filepath.SkipDir
			}
			return os.MkdirAll(filepath.Join(probeOpenClaw, rel), 0o700)
		}
		if !wanted {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if isControl || isSession {
				return fmt.Errorf("upgrade probe input %s is a symlink", slash)
			}
			return nil
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return infoErr
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if err := copyRegularFileStable(path, filepath.Join(probeOpenClaw, rel), 0o600); err != nil {
			return err
		}
		if isControl {
			stateBytes += info.Size()
		}
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", "", 0, err
	}
	for source, target := range workspaceCopies {
		if err := os.MkdirAll(target, 0o700); err != nil {
			return "", "", 0, err
		}
		for _, name := range []string{"HEARTBEAT.md", "TOOLS.md"} {
			sourcePath := filepath.Join(source, name)
			if info, statErr := os.Lstat(sourcePath); statErr == nil && info.Mode().IsRegular() {
				if err := copyRegularFileStable(sourcePath, filepath.Join(target, name), 0o600); err != nil {
					return "", "", 0, err
				}
			}
		}
	}
	if err := chownTree(probeRoot, req.UID, req.GID); err != nil {
		return "", "", 0, err
	}
	ok = true
	return probeRoot, probeHome, stateBytes, nil
}

func remapProbeWorkspacePaths(config map[string]any, probeOpenClaw string) map[string]string {
	copies := map[string]string{}
	var walk func(any, string)
	walk = func(value any, key string) {
		switch typed := value.(type) {
		case map[string]any:
			for childKey, child := range typed {
				lower := strings.ToLower(strings.TrimSpace(childKey))
				if text, ok := child.(string); ok && (lower == "workspace" || lower == "workspacepath" || lower == "workspace_path") {
					digest := sha256.Sum256([]byte(text))
					target := filepath.Join(probeOpenClaw, "workspace-probes", hex.EncodeToString(digest[:6]))
					typed[childKey] = target
					if filepath.IsAbs(strings.TrimSpace(text)) {
						copies[filepath.Clean(text)] = target
					}
					continue
				}
				walk(child, childKey)
			}
		case []any:
			for _, child := range typed {
				walk(child, key)
			}
		}
	}
	walk(config, "")
	return copies
}

func copyRegularFileStable(source, target string, mode os.FileMode) error {
	for attempt := 0; attempt < 3; attempt++ {
		before, err := os.Lstat(source)
		if err != nil {
			return err
		}
		if !before.Mode().IsRegular() {
			return fmt.Errorf("upgrade probe source is not a regular file: %s", source)
		}
		if err := copyRegularFile(source, target, mode); err != nil {
			return err
		}
		after, err := os.Lstat(source)
		if err != nil {
			return err
		}
		if before.Size() == after.Size() && before.ModTime().Equal(after.ModTime()) {
			return nil
		}
		_ = os.Remove(target)
	}
	return errors.New("upgrade probe source remained active; retry after current write completes")
}

func chownTree(root string, uid, gid int) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		return ChownWorkspace(path, uid, gid)
	})
}

func removeUpgradeProbe(path string) {
	clean := filepath.Clean(path)
	base := filepath.Clean(os.TempDir())
	if strings.HasPrefix(filepath.Base(clean), "clawmanager-upgrade-") && pathWithin(base, clean) {
		_ = os.RemoveAll(clean)
	}
}

func runOpenClawCommandInHome(ctx context.Context, dir, home, configPath string, req WorkspaceUpgradeRequest, phase string, args []string, hash io.Writer, timeout time.Duration) error {
	phaseCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stateDir := filepath.Join(home, ".openclaw")
	tempRoot := filepath.Join(stateDir, "tmp")
	scratchRoot := filepath.Join(tempRoot, "clawmanager-upgrade-"+req.RolloutID)
	cacheRoot := filepath.Join(stateDir, "cache")
	xdgStateRoot := filepath.Join(stateDir, "xdg-state")
	for _, path := range []string{tempRoot, scratchRoot, cacheRoot, xdgStateRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
		if err := ChownWorkspace(path, req.UID, req.GID); err != nil {
			return err
		}
	}
	cmd := exec.CommandContext(phaseCtx, "openclaw", args...)
	cmd.Dir = dir
	cmd.Env = upgradeCommandEnv(map[string]string{
		"HOME": home, "OPENCLAW_STATE_DIR": stateDir, "OPENCLAW_CONFIG_PATH": configPath,
		"TMPDIR": scratchRoot, "XDG_CACHE_HOME": cacheRoot,
		"XDG_STATE_HOME": xdgStateRoot, "NO_COLOR": "1",
	})
	configureGatewayCommand(cmd, req.UID, req.GID)
	output, runErr := cmd.CombinedOutput()
	if len(output) > 64*1024 {
		output = output[:64*1024]
	}
	_, _ = hash.Write([]byte(phase))
	_, _ = hash.Write(output)
	if runErr == nil {
		return nil
	}
	digest := sha256.Sum256(output)
	failure := fmt.Errorf("OPENCLAW_UPGRADE_%s_FAILED: %s (output_sha256=%x): %w", strings.ToUpper(strings.ReplaceAll(phase, "-", "_")), safeOpenClawFailureSummary(output), digest, runErr)
	lower := strings.ToLower(string(output))
	if strings.Contains(lower, "database is locked") || strings.Contains(lower, "sqlite_busy") || strings.Contains(lower, "resource temporarily unavailable") {
		return errors.Join(errUpgradeProbeBusy, failure)
	}
	return failure
}

func upgradeCommandEnv(overrides map[string]string) []string {
	env := make([]string, 0, len(os.Environ())+len(overrides))
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if _, replaced := overrides[key]; !replaced {
			env = append(env, item)
		}
	}
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		env = append(env, key+"="+overrides[key])
	}
	return env
}

// MigrateSessionSQLite executes OpenClaw's public, transactional migration
// workflow against the fixed persistent HOME. It is only callable while the
// gateway is stopped and the control plane owns the workspace writer lease.
func (m *GatewayManager) MigrateSessionSQLite(ctx context.Context, req WorkspaceUpgradeRequest) (SessionSQLiteMigrationResult, error) {
	result := SessionSQLiteMigrationResult{InstanceID: req.InstanceID, PhaseDurationsMS: map[string]int64{}}
	if err := validateUpgradeID("rollout_id", req.RolloutID); err != nil {
		return result, err
	}
	workspace, err := m.workspaceForUpgrade(req)
	if err != nil {
		return result, err
	}
	if !IsOpenClawAtLeast(m.cfg.OpenClawVersion, OpenClaw81Version) {
		return result, fmt.Errorf("OpenClaw %s does not support session SQLite migration", m.cfg.OpenClawVersion)
	}
	if m.InstanceActive(req.InstanceID) {
		return result, fmt.Errorf("instance %d still has an active gateway", req.InstanceID)
	}
	if _, err := m.requireWriterLease(req); err != nil {
		return result, err
	}
	if req.UID <= 0 || req.GID <= 0 {
		return result, errors.New("positive uid and gid are required for session migration")
	}
	receiptPath := m.sessionMigrationReceiptPath(req)
	if raw, readErr := os.ReadFile(receiptPath); readErr == nil {
		if jsonErr := json.Unmarshal(raw, &result); jsonErr == nil && result.InstanceID == req.InstanceID && result.Status == "validated" {
			return result, nil
		}
	}
	home := filepath.Join(workspace, "home")
	if info, err := os.Stat(home); err != nil || !info.IsDir() {
		return result, fmt.Errorf("persistent HOME is unavailable: %w", err)
	}
	stateCapsule, err := m.prepareOpenClawStateCapsule(workspace, req)
	if err != nil {
		return result, err
	}
	result.StateCapsuleBytes = stateCapsule.TotalBytes
	capsule, err := m.prepareOpenClaw81Config(workspace, req)
	if err != nil {
		return result, err
	}
	result.ConfigOriginalSHA256 = capsule.OriginalSHA256
	result.ConfigTargetSHA256 = capsule.TargetSHA256

	hash := sha256.New()
	phaseStarted := time.Now()
	if err := runOpenClawUpgradeCommand(ctx, workspace, home, req, "doctor-fix", []string{"doctor", "--fix", "--yes", "--non-interactive"}, hash); err != nil {
		result.PhaseDurationsMS["doctor_fix"] = time.Since(phaseStarted).Milliseconds()
		result.Status = "failed_doctor_fix"
		result.OutputSHA256 = hex.EncodeToString(hash.Sum(nil))
		result.CompletedAt = time.Now().UTC()
		if receiptErr := writeSessionMigrationReceipt(receiptPath, result); receiptErr != nil {
			return result, errors.Join(err, fmt.Errorf("persist failed migration receipt: %w", receiptErr))
		}
		return result, err
	}
	result.PhaseDurationsMS["doctor_fix"] = time.Since(phaseStarted).Milliseconds()
	phaseStarted = time.Now()
	if err := runOpenClawUpgradeCommand(ctx, workspace, home, req, "config-validate", []string{"config", "validate", "--json"}, hash); err != nil {
		result.PhaseDurationsMS["config_validate"] = time.Since(phaseStarted).Milliseconds()
		result.Status = "failed_config_validate"
		result.OutputSHA256 = hex.EncodeToString(hash.Sum(nil))
		result.CompletedAt = time.Now().UTC()
		if receiptErr := writeSessionMigrationReceipt(receiptPath, result); receiptErr != nil {
			return result, errors.Join(err, fmt.Errorf("persist failed migration receipt: %w", receiptErr))
		}
		return result, err
	}
	result.PhaseDurationsMS["config_validate"] = time.Since(phaseStarted).Milliseconds()
	for _, args := range sessionSQLiteMigrationCommands() {
		phase := args[2]
		phaseStarted = time.Now()
		if err := runOpenClawUpgradeCommand(ctx, workspace, home, req, phase, args, hash); err != nil {
			result.PhaseDurationsMS[strings.ReplaceAll(phase, "-", "_")] = time.Since(phaseStarted).Milliseconds()
			result.Status = "failed_" + strings.ReplaceAll(phase, "-", "_")
			result.OutputSHA256 = hex.EncodeToString(hash.Sum(nil))
			result.ArchiveBytes, result.ArchiveFiles, _ = sessionMigrationArchiveStats(home)
			result.RollbackAvailable = result.ArchiveFiles > 0
			result.CompletedAt = time.Now().UTC()
			if receiptErr := writeSessionMigrationReceipt(receiptPath, result); receiptErr != nil {
				return result, errors.Join(err, fmt.Errorf("persist failed migration receipt: %w", receiptErr))
			}
			return result, err
		}
		result.PhaseDurationsMS[strings.ReplaceAll(phase, "-", "_")] = time.Since(phaseStarted).Milliseconds()
	}
	result.Status = "validated"
	result.OutputSHA256 = hex.EncodeToString(hash.Sum(nil))
	result.ArchiveBytes, result.ArchiveFiles, err = sessionMigrationArchiveStats(home)
	if err != nil {
		return result, err
	}
	result.RollbackAvailable = result.ArchiveFiles > 0
	phaseStarted = time.Now()
	result.SessionCount, result.SessionCatalogSHA256, err = inspectOfficialSessionCatalog(ctx, workspace, home, req)
	result.PhaseDurationsMS["session_catalog"] = time.Since(phaseStarted).Milliseconds()
	if err != nil {
		result.Status = "failed_session_catalog"
		result.CompletedAt = time.Now().UTC()
		if receiptErr := writeSessionMigrationReceipt(receiptPath, result); receiptErr != nil {
			return result, errors.Join(err, fmt.Errorf("persist failed migration receipt: %w", receiptErr))
		}
		return result, err
	}
	result.CompletedAt = time.Now().UTC()
	if err := writeSessionMigrationReceipt(receiptPath, result); err != nil {
		return result, err
	}
	return result, nil
}

// SessionSQLiteMigrationStatus returns only the durable receipt written by the
// migration operation. It never starts or repeats a migration, which lets the
// control plane safely reconcile a lost HTTP response.
func (m *GatewayManager) SessionSQLiteMigrationStatus(req WorkspaceUpgradeRequest) (SessionSQLiteMigrationResult, error) {
	var result SessionSQLiteMigrationResult
	if err := validateUpgradeID("rollout_id", req.RolloutID); err != nil {
		return result, err
	}
	if _, err := m.workspaceForUpgrade(req); err != nil {
		return result, err
	}
	raw, err := os.ReadFile(m.sessionMigrationReceiptPath(req))
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return result, fmt.Errorf("invalid session migration receipt: %w", err)
	}
	if result.InstanceID != req.InstanceID || strings.TrimSpace(result.Status) == "" {
		return result, errors.New("session migration receipt identity is invalid")
	}
	return result, nil
}

func inspectOfficialSessionCatalog(ctx context.Context, workspace, home string, req WorkspaceUpgradeRequest) (int, string, error) {
	phaseCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	stateDir := filepath.Join(home, ".openclaw")
	cmd := exec.CommandContext(phaseCtx, "openclaw", "sessions", "--all-agents", "--json", "--limit", "all")
	cmd.Dir = workspace
	cmd.Env = upgradeCommandEnv(map[string]string{
		"HOME": home, "OPENCLAW_STATE_DIR": stateDir,
		"OPENCLAW_CONFIG_PATH": filepath.Join(stateDir, "openclaw.json"), "NO_COLOR": "1",
	})
	configureGatewayCommand(cmd, req.UID, req.GID)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		output := append(append([]byte(nil), stdout.Bytes()...), stderr.Bytes()...)
		digest := sha256.Sum256(output)
		return 0, "", fmt.Errorf("OPENCLAW_UPGRADE_SESSION_CATALOG_FAILED: %s (output_sha256=%x): %w", safeOpenClawFailureSummary(output), digest, err)
	}
	count, digest, parseErr := canonicalSessionCatalog(stdout.Bytes())
	if parseErr == nil {
		return count, digest, nil
	}
	output := append(append([]byte(nil), stdout.Bytes()...), stderr.Bytes()...)
	outputDigest := sha256.Sum256(output)
	return 0, "", fmt.Errorf("OPENCLAW_UPGRADE_SESSION_CATALOG_INVALID: output_sha256=%x: %w", outputDigest, parseErr)
}

func canonicalSessionCatalog(raw []byte) (int, string, error) {
	catalogs := make([]openClawSessionCatalog, 0, 1)
	for offset := 0; offset < len(raw); {
		start := bytes.IndexByte(raw[offset:], '{')
		if start < 0 {
			break
		}
		start += offset
		decoder := json.NewDecoder(bytes.NewReader(raw[start:]))
		var candidate map[string]json.RawMessage
		if err := decoder.Decode(&candidate); err != nil {
			offset = start + 1
			continue
		}
		consumed := int(decoder.InputOffset())
		if _, ok := candidate["sessions"]; ok {
			encoded, err := json.Marshal(candidate)
			if err != nil {
				return 0, "", fmt.Errorf("OPENCLAW_UPGRADE_SESSION_CATALOG_INVALID: %w", err)
			}
			var catalog openClawSessionCatalog
			if err := json.Unmarshal(encoded, &catalog); err != nil {
				return 0, "", fmt.Errorf("OPENCLAW_UPGRADE_SESSION_CATALOG_INVALID: %w", err)
			}
			catalogs = append(catalogs, catalog)
		}
		if consumed <= 0 {
			offset = start + 1
		} else {
			offset = start + consumed
		}
	}
	if len(catalogs) != 1 {
		return 0, "", fmt.Errorf("OPENCLAW_UPGRADE_SESSION_CATALOG_INVALID: expected exactly one session catalog, found %d", len(catalogs))
	}
	catalog := catalogs[0]
	entries := make([]string, 0, len(catalog.Sessions))
	for _, session := range catalog.Sessions {
		key, sessionID := strings.TrimSpace(session.Key), strings.TrimSpace(session.SessionID)
		if key == "" || sessionID == "" {
			return 0, "", errors.New("OPENCLAW_UPGRADE_SESSION_CATALOG_INVALID: session key or id is empty")
		}
		entries = append(entries, key+"\x00"+sessionID+"\n")
	}
	sort.Strings(entries)
	hash := sha256.New()
	for _, entry := range entries {
		_, _ = io.WriteString(hash, entry)
	}
	return len(entries), hex.EncodeToString(hash.Sum(nil)), nil
}

func writeSessionMigrationReceipt(path string, result SessionSQLiteMigrationResult) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	raw, _ := json.Marshal(result)
	return atomicWriteFile(path, append(raw, '\n'), 0o600)
}

func (m *GatewayManager) sessionMigrationReceiptPath(req WorkspaceUpgradeRequest) string {
	return filepath.Join(m.upgradeRoot(), "rollout-"+req.RolloutID, "instance-"+strconv.Itoa(req.InstanceID), "session-sqlite-migration.json")
}

func (m *GatewayManager) sessionRestoreReceiptPath(req WorkspaceUpgradeRequest) string {
	return filepath.Join(m.upgradeRoot(), "rollout-"+req.RolloutID, "instance-"+strconv.Itoa(req.InstanceID), "session-sqlite-restore.json")
}

// RestoreSessionSQLite invokes OpenClaw's supported rollback workflow. It only
// restores the upstream migration archive; project files and the rest of the
// persistent workspace are never copied or replaced.
func (m *GatewayManager) RestoreSessionSQLite(ctx context.Context, req WorkspaceUpgradeRequest) (SessionSQLiteRestoreResult, error) {
	result := SessionSQLiteRestoreResult{InstanceID: req.InstanceID, PhaseDurationsMS: map[string]int64{}}
	if err := validateUpgradeID("rollout_id", req.RolloutID); err != nil {
		return result, err
	}
	workspace, err := m.workspaceForUpgrade(req)
	if err != nil {
		return result, err
	}
	if !IsOpenClawAtLeast(m.cfg.OpenClawVersion, OpenClaw81Version) {
		return result, fmt.Errorf("OpenClaw %s does not support session SQLite restore", m.cfg.OpenClawVersion)
	}
	if m.InstanceActive(req.InstanceID) {
		return result, fmt.Errorf("instance %d still has an active gateway", req.InstanceID)
	}
	if _, err := m.requireWriterLease(req); err != nil {
		return result, err
	}
	if req.UID <= 0 || req.GID <= 0 {
		return result, errors.New("positive uid and gid are required for session restore")
	}
	receiptPath := m.sessionRestoreReceiptPath(req)
	if raw, readErr := os.ReadFile(receiptPath); readErr == nil {
		if jsonErr := json.Unmarshal(raw, &result); jsonErr == nil && sessionRestoreReceiptMatches(result, req) {
			return result, nil
		}
	}
	migration := SessionSQLiteMigrationResult{}
	raw, migrationReadErr := os.ReadFile(m.sessionMigrationReceiptPath(req))
	home := filepath.Join(workspace, "home")
	hasMigrationArchive := !req.PreserveSessionSQLite && migrationReadErr == nil && json.Unmarshal(raw, &migration) == nil && migration.RollbackAvailable
	// The upstream importer can move JSONL files before a process or storage
	// failure prevents our receipt from being written. Discovering the official
	// archive closes that crash window without copying or guessing user data.
	if !req.PreserveSessionSQLite && !hasMigrationArchive {
		_, archiveFiles, archiveErr := sessionMigrationArchiveStats(home)
		if archiveErr != nil {
			return result, fmt.Errorf("inspect session migration archive: %w", archiveErr)
		}
		hasMigrationArchive = archiveFiles > 0
	}
	hash := sha256.New()
	if hasMigrationArchive {
		args := []string{"doctor", "--session-sqlite", "restore", "--session-sqlite-all-agents", "--json", "--non-interactive", "--yes"}
		phaseStarted := time.Now()
		if err := runOpenClawUpgradeCommand(ctx, workspace, home, req, "restore", args, hash); err != nil {
			result.PhaseDurationsMS["session_restore"] = time.Since(phaseStarted).Milliseconds()
			return result, err
		}
		result.PhaseDurationsMS["session_restore"] = time.Since(phaseStarted).Milliseconds()
	}
	phaseStarted := time.Now()
	configRestored, configErr := m.restoreOpenClawConfig(workspace, req)
	result.PhaseDurationsMS["config_restore"] = time.Since(phaseStarted).Milliseconds()
	if configErr != nil {
		return result, configErr
	}
	phaseStarted = time.Now()
	stateRestored, stateErr := m.restoreOpenClawStateCapsule(workspace, req)
	result.PhaseDurationsMS["state_restore"] = time.Since(phaseStarted).Milliseconds()
	if stateErr != nil {
		return result, stateErr
	}
	if !hasMigrationArchive && !configRestored && !stateRestored {
		return result, errors.New("upgrade rollback capsule is unavailable")
	}
	result.Status = "restored"
	result.OutputSHA256 = hex.EncodeToString(hash.Sum(nil))
	result.ConfigRestored = configRestored
	result.StateRestored = stateRestored
	result.PreservedSessionSQLite = req.PreserveSessionSQLite
	result.CompletedAt = time.Now().UTC()
	if err := os.MkdirAll(filepath.Dir(receiptPath), 0o700); err != nil {
		return result, err
	}
	raw, _ = json.Marshal(result)
	if err := atomicWriteFile(receiptPath, append(raw, '\n'), 0o600); err != nil {
		return result, err
	}
	return result, nil
}

func sessionRestoreReceiptMatches(result SessionSQLiteRestoreResult, req WorkspaceUpgradeRequest) bool {
	return result.InstanceID == req.InstanceID && result.Status == "restored" && result.PreservedSessionSQLite == req.PreserveSessionSQLite
}

// SessionSQLiteRestoreStatus is the read-only counterpart of restore. It is
// used only to resolve an ambiguous transport failure.
func (m *GatewayManager) SessionSQLiteRestoreStatus(req WorkspaceUpgradeRequest) (SessionSQLiteRestoreResult, error) {
	var result SessionSQLiteRestoreResult
	if err := validateUpgradeID("rollout_id", req.RolloutID); err != nil {
		return result, err
	}
	if _, err := m.workspaceForUpgrade(req); err != nil {
		return result, err
	}
	raw, err := os.ReadFile(m.sessionRestoreReceiptPath(req))
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return result, fmt.Errorf("invalid session restore receipt: %w", err)
	}
	if result.InstanceID != req.InstanceID || strings.TrimSpace(result.Status) == "" {
		return result, errors.New("session restore receipt identity is invalid")
	}
	return result, nil
}

func runOpenClawUpgradeCommand(ctx context.Context, workspace, home string, req WorkspaceUpgradeRequest, phase string, args []string, hash io.Writer) error {
	return runOpenClawCommandInHome(ctx, workspace, home, filepath.Join(home, ".openclaw", "openclaw.json"), req, phase, args, hash, 10*time.Minute)
}

func safeOpenClawFailureSummary(output []byte) string {
	var safe []string
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		lower := strings.ToLower(line)
		if line == "" || (!strings.Contains(lower, "error") && !strings.Contains(lower, "failed") && !strings.Contains(lower, "reason") && !strings.Contains(lower, "unrecognized key") && !strings.Contains(lower, "invalid config") && !strings.Contains(lower, "config validation failed") && !strings.Contains(lower, "schema migration") && !strings.Contains(lower, "legacy agent database") && !strings.Contains(lower, "database is locked") && !strings.Contains(lower, "sqlite_busy") && !strings.Contains(lower, "transcript_malformed") && !strings.Contains(lower, "transcript_missing")) {
			continue
		}
		line = regexp.MustCompile(`/[^ ]+`).ReplaceAllString(line, "<path>")
		line = regexp.MustCompile(`(?i)(token|password|secret)([=: ]+)[^,; ]+`).ReplaceAllString(line, "$1$2<redacted>")
		line = regexp.MustCompile(`\b[0-9a-fA-F]{40,}\b`).ReplaceAllString(line, "<digest>")
		if len(line) > 240 {
			line = line[:240]
		}
		safe = append(safe, line)
		if len(safe) == 4 {
			break
		}
	}
	if len(safe) == 0 {
		return "target OpenClaw rejected the configuration or session migration input"
	}
	return strings.Join(safe, "; ")
}

func (m *GatewayManager) configCapsuleDir(req WorkspaceUpgradeRequest) string {
	return filepath.Join(m.upgradeRoot(), "rollout-"+req.RolloutID, "instance-"+strconv.Itoa(req.InstanceID))
}

func (m *GatewayManager) prepareOpenClaw81Config(workspace string, req WorkspaceUpgradeRequest) (upgradeConfigCapsule, error) {
	var capsule upgradeConfigCapsule
	configPath := filepath.Join(workspace, "home", ".openclaw", "openclaw.json")
	raw, readErr := os.ReadFile(configPath)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return capsule, fmt.Errorf("read OpenClaw config: %w", readErr)
	}
	if len(raw) > maxUpgradeConfigBytes {
		return capsule, fmt.Errorf("OpenClaw config exceeds %d byte upgrade limit", maxUpgradeConfigBytes)
	}
	originalExists := readErr == nil
	originalMode := uint32(0o600)
	if info, statErr := os.Stat(configPath); statErr == nil {
		originalMode = uint32(info.Mode().Perm())
	}
	config := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &config); err != nil {
			return capsule, fmt.Errorf("parse OpenClaw config: %w", err)
		}
	}
	openclawcompat.Normalize81(config)
	target, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return capsule, fmt.Errorf("marshal normalized OpenClaw config: %w", err)
	}
	target = append(target, '\n')
	originalHash := sha256.Sum256(raw)
	targetHash := sha256.Sum256(target)
	capsule = upgradeConfigCapsule{SchemaVersion: 1, InstanceID: req.InstanceID, OriginalExists: originalExists, OriginalMode: originalMode, OriginalSHA256: hex.EncodeToString(originalHash[:]), TargetSHA256: hex.EncodeToString(targetHash[:])}
	dir := m.configCapsuleDir(req)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return capsule, err
	}
	manifestPath := filepath.Join(dir, "config-capsule.json")
	if existing, err := os.ReadFile(manifestPath); err == nil {
		var prior upgradeConfigCapsule
		if json.Unmarshal(existing, &prior) != nil || prior.InstanceID != req.InstanceID {
			return capsule, errors.New("invalid existing OpenClaw config upgrade capsule")
		}
		currentSHA := hex.EncodeToString(originalHash[:])
		if currentSHA == prior.TargetSHA256 {
			return prior, nil
		}
		if currentSHA != prior.OriginalSHA256 || capsule.TargetSHA256 != prior.TargetSHA256 {
			return capsule, errors.New("OpenClaw config changed after the upgrade capsule was created")
		}
		capsule = prior
	} else {
		if originalExists {
			backupPath := filepath.Join(dir, "openclaw.json.gz")
			file, createErr := os.OpenFile(backupPath+".tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if createErr != nil {
				return capsule, createErr
			}
			gz := gzip.NewWriter(file)
			_, writeErr := gz.Write(raw)
			closeErr := errors.Join(gz.Close(), file.Sync(), file.Close())
			if err := errors.Join(writeErr, closeErr); err != nil {
				_ = os.Remove(backupPath + ".tmp")
				return capsule, err
			}
			if err := os.Rename(backupPath+".tmp", backupPath); err != nil {
				return capsule, err
			}
		}
		manifest, _ := json.Marshal(capsule)
		if err := atomicWriteFile(manifestPath, append(manifest, '\n'), 0o600); err != nil {
			return capsule, err
		}
	}
	if err := atomicWriteFile(configPath, target, 0o600); err != nil {
		return capsule, fmt.Errorf("write normalized OpenClaw config: %w", err)
	}
	if err := ChownWorkspace(configPath, req.UID, req.GID); err != nil {
		return capsule, fmt.Errorf("chown normalized OpenClaw config: %w", err)
	}
	return capsule, nil
}

func (m *GatewayManager) restoreOpenClawConfig(workspace string, req WorkspaceUpgradeRequest) (bool, error) {
	dir := m.configCapsuleDir(req)
	manifestRaw, err := os.ReadFile(filepath.Join(dir, "config-capsule.json"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var capsule upgradeConfigCapsule
	if err := json.Unmarshal(manifestRaw, &capsule); err != nil || capsule.InstanceID != req.InstanceID {
		return false, errors.New("invalid OpenClaw config rollback capsule")
	}
	configPath := filepath.Join(workspace, "home", ".openclaw", "openclaw.json")
	if !capsule.OriginalExists {
		if err := os.Remove(configPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		return true, nil
	}
	file, err := os.Open(filepath.Join(dir, "openclaw.json.gz"))
	if err != nil {
		return false, err
	}
	gz, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return false, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(gz, maxUpgradeConfigBytes+1))
	closeErr := errors.Join(gz.Close(), file.Close())
	if err := errors.Join(readErr, closeErr); err != nil {
		return false, err
	}
	if len(raw) > maxUpgradeConfigBytes {
		return false, errors.New("OpenClaw config rollback capsule exceeds limit")
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != capsule.OriginalSHA256 {
		return false, errors.New("OpenClaw config rollback capsule checksum mismatch")
	}
	mode := os.FileMode(capsule.OriginalMode)
	if mode == 0 {
		mode = 0o600
	}
	if err := atomicWriteFile(configPath, raw, mode); err != nil {
		return false, err
	}
	if err := ChownWorkspace(configPath, req.UID, req.GID); err != nil {
		return false, err
	}
	return true, nil
}

func (m *GatewayManager) prepareOpenClawStateCapsule(workspace string, req WorkspaceUpgradeRequest) (upgradeStateCapsule, error) {
	dir := m.configCapsuleDir(req)
	manifestPath := filepath.Join(dir, "state-capsule.json")
	if raw, err := os.ReadFile(manifestPath); err == nil {
		var existing upgradeStateCapsule
		if json.Unmarshal(raw, &existing) != nil || existing.InstanceID != req.InstanceID || existing.SchemaVersion != 1 {
			return upgradeStateCapsule{}, errors.New("invalid existing OpenClaw state upgrade capsule")
		}
		return existing, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return upgradeStateCapsule{}, err
	}
	openClawDir := filepath.Join(workspace, "home", ".openclaw")
	capsule := upgradeStateCapsule{SchemaVersion: 1, InstanceID: req.InstanceID, WorkspaceFiles: map[string]bool{}, ControlDirs: map[string]bool{}}
	for _, name := range []string{"state", "cron", "tasks", "automation", "automations"} {
		source := filepath.Join(openClawDir, name)
		info, statErr := os.Stat(source)
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return upgradeStateCapsule{}, statErr
		}
		exists := statErr == nil && info.IsDir()
		capsule.ControlDirs[name] = exists
		if name == "state" {
			capsule.OriginalExists = exists
		}
		if !exists {
			continue
		}
		bytes, files, err := copyTreeStable(source, filepath.Join(dir, "control-original", name))
		if err != nil {
			return upgradeStateCapsule{}, fmt.Errorf("capture OpenClaw %s capsule: %w", name, err)
		}
		capsule.TotalBytes += bytes
		capsule.FileCount += files
	}
	workspaceDir := filepath.Join(workspace, "home", ".openclaw", "workspace")
	workspaceCapsule := filepath.Join(dir, "workspace-original")
	if err := os.MkdirAll(workspaceCapsule, 0o700); err != nil {
		return upgradeStateCapsule{}, err
	}
	for _, name := range []string{"HEARTBEAT.md", "TOOLS.md"} {
		source := filepath.Join(workspaceDir, name)
		fileInfo, err := os.Lstat(source)
		if errors.Is(err, os.ErrNotExist) {
			capsule.WorkspaceFiles[name] = false
			continue
		}
		if err != nil {
			return upgradeStateCapsule{}, err
		}
		if !fileInfo.Mode().IsRegular() {
			return upgradeStateCapsule{}, fmt.Errorf("OpenClaw workspace migration input %s is not a regular file", name)
		}
		capsule.WorkspaceFiles[name] = true
		if err := copyRegularFileStable(source, filepath.Join(workspaceCapsule, name), fileInfo.Mode().Perm()); err != nil {
			return upgradeStateCapsule{}, err
		}
		capsule.TotalBytes += fileInfo.Size()
		capsule.FileCount++
	}
	raw, _ := json.Marshal(capsule)
	if err := atomicWriteFile(manifestPath, append(raw, '\n'), 0o600); err != nil {
		return upgradeStateCapsule{}, err
	}
	return capsule, nil
}

func copyTreeStable(source, target string) (int64, int64, error) {
	var totalBytes, files int64
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(source, path)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return ErrWorkspacePath
		}
		destination := filepath.Join(target, rel)
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("state capsule refuses symlink %s", rel)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o700)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("state capsule refuses non-regular file %s", rel)
		}
		if err := copyRegularFileStable(path, destination, info.Mode().Perm()); err != nil {
			return err
		}
		totalBytes += info.Size()
		files++
		return nil
	})
	return totalBytes, files, err
}

func (m *GatewayManager) restoreOpenClawStateCapsule(workspace string, req WorkspaceUpgradeRequest) (bool, error) {
	dir := m.configCapsuleDir(req)
	raw, err := os.ReadFile(filepath.Join(dir, "state-capsule.json"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var capsule upgradeStateCapsule
	if json.Unmarshal(raw, &capsule) != nil || capsule.InstanceID != req.InstanceID || capsule.SchemaVersion != 1 {
		return false, errors.New("invalid OpenClaw state rollback capsule")
	}
	openClawDir := filepath.Join(workspace, "home", ".openclaw")
	for _, name := range []string{"state", "cron", "tasks", "automation", "automations"} {
		target := filepath.Join(openClawDir, name)
		failed := filepath.Join(dir, "control-failed-target", name)
		if _, err := os.Stat(failed); errors.Is(err, os.ErrNotExist) {
			if _, currentErr := os.Stat(target); currentErr == nil {
				if err := os.MkdirAll(filepath.Dir(failed), 0o700); err != nil {
					return false, err
				}
				if err := os.Rename(target, failed); err != nil {
					return false, fmt.Errorf("quarantine target OpenClaw %s: %w", name, err)
				}
			} else if !errors.Is(currentErr, os.ErrNotExist) {
				return false, currentErr
			}
		}
		if capsule.ControlDirs[name] {
			original := filepath.Join(dir, "control-original", name)
			if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) {
				if err := os.Rename(original, target); err != nil {
					return false, fmt.Errorf("restore original OpenClaw %s: %w", name, err)
				}
			} else if err != nil {
				return false, err
			}
			if err := chownTree(target, req.UID, req.GID); err != nil {
				return false, err
			}
		}
	}
	workspaceDir := filepath.Join(workspace, "home", ".openclaw", "workspace")
	failedWorkspace := filepath.Join(dir, "workspace-failed-target")
	for _, name := range []string{"HEARTBEAT.md", "TOOLS.md"} {
		target := filepath.Join(workspaceDir, name)
		if capsule.WorkspaceFiles[name] {
			source := filepath.Join(dir, "workspace-original", name)
			info, err := os.Stat(source)
			if err != nil {
				return false, err
			}
			data, err := os.ReadFile(source)
			if err != nil {
				return false, err
			}
			if err := atomicWriteFile(target, data, info.Mode().Perm()); err != nil {
				return false, err
			}
			if err := ChownWorkspace(target, req.UID, req.GID); err != nil {
				return false, err
			}
			continue
		}
		if _, err := os.Stat(target); err == nil {
			if err := os.MkdirAll(failedWorkspace, 0o700); err != nil {
				return false, err
			}
			if err := os.Rename(target, filepath.Join(failedWorkspace, name)); err != nil {
				return false, err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return true, nil
}

func sessionMigrationArchiveStats(home string) (int64, int64, error) {
	var bytes, files int64
	err := filepath.WalkDir(filepath.Join(home, ".openclaw"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		rel, relErr := filepath.Rel(filepath.Join(home, ".openclaw"), path)
		if relErr != nil {
			return relErr
		}
		slash := filepath.ToSlash(rel)
		if !strings.Contains(slash, "session-sqlite-import-archive/") && !strings.Contains(slash, "session-sqlite-migration-runs/") {
			return nil
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return infoErr
		}
		files++
		bytes += info.Size()
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	return bytes, files, err
}

func sessionSQLiteMigrationCommands() [][]string {
	return [][]string{
		{"doctor", "--session-sqlite", "dry-run", "--session-sqlite-all-agents", "--json", "--non-interactive"},
		{"doctor", "--session-sqlite", "import", "--session-sqlite-all-agents", "--json", "--non-interactive", "--yes"},
		{"doctor", "--session-sqlite", "validate", "--session-sqlite-all-agents", "--json", "--non-interactive"},
	}
}

type WorkspaceInventory struct {
	WorkspacePath  string              `json:"workspace_path"`
	OpenClawHome   string              `json:"openclaw_home"`
	FileCount      int64               `json:"file_count"`
	DirectoryCount int64               `json:"directory_count"`
	SymlinkCount   int64               `json:"symlink_count"`
	TotalBytes     int64               `json:"total_bytes"`
	AvailableBytes uint64              `json:"available_bytes"`
	DatabaseFiles  []DatabasePreflight `json:"database_files"`
	CheckedAt      time.Time           `json:"checked_at"`
}

type DatabasePreflight struct {
	RelativePath string `json:"relative_path"`
	SizeBytes    int64  `json:"size_bytes"`
	SQLiteHeader bool   `json:"sqlite_header"`
	OfficialOK   bool   `json:"official_ok,omitempty"`
	OutputSHA256 string `json:"output_sha256,omitempty"`
}

type SnapshotManifest struct {
	SchemaVersion int                `json:"schema_version"`
	RolloutID     string             `json:"rollout_id"`
	SnapshotID    string             `json:"snapshot_id"`
	InstanceID    int                `json:"instance_id"`
	UserID        int                `json:"user_id"`
	Generation    int                `json:"generation"`
	WorkspacePath string             `json:"workspace_path"`
	ArchivePath   string             `json:"archive_path"`
	ArchiveSHA256 string             `json:"archive_sha256"`
	ArchiveBytes  int64              `json:"archive_bytes"`
	FileCount     int64              `json:"file_count"`
	TotalBytes    int64              `json:"total_bytes"`
	CreatedAt     time.Time          `json:"created_at"`
	Files         []SnapshotFileFact `json:"files"`
}

type SnapshotFileFact struct {
	Path    string      `json:"path"`
	Mode    fs.FileMode `json:"mode"`
	Size    int64       `json:"size"`
	ModTime time.Time   `json:"mod_time"`
	SHA256  string      `json:"sha256,omitempty"`
	Link    string      `json:"link,omitempty"`
}

type WriterLease struct {
	SchemaVersion int       `json:"schema_version"`
	RolloutID     string    `json:"rollout_id"`
	InstanceID    int       `json:"instance_id"`
	UserID        int       `json:"user_id"`
	Generation    int       `json:"generation"`
	Token         string    `json:"token"`
	Holder        string    `json:"holder"`
	ExpiresAt     time.Time `json:"expires_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (m *GatewayManager) workspaceForUpgrade(req WorkspaceUpgradeRequest) (string, error) {
	if m.cfg.RuntimeType != "openclaw" {
		return "", ErrRuntimeType
	}
	if req.UserID <= 0 || req.InstanceID <= 0 || req.Generation <= 0 {
		return "", fmt.Errorf("%w: positive user, instance and generation are required", ErrWorkspacePath)
	}
	return ValidateWorkspacePath(m.cfg.WorkspaceRoot, m.cfg.RuntimeType, CreateGatewayRequest{
		UserID: req.UserID, InstanceID: req.InstanceID,
		WorkspacePath: filepath.Join(m.cfg.WorkspaceRoot, m.cfg.RuntimeType, "user-"+strconv.Itoa(req.UserID), "instance-"+strconv.Itoa(req.InstanceID)),
	})
}

func validateUpgradeID(label, value string) error {
	if !safeUpgradeID.MatchString(strings.TrimSpace(value)) {
		return fmt.Errorf("invalid %s", label)
	}
	return nil
}

func (m *GatewayManager) PreflightWorkspace(ctx context.Context, req WorkspaceUpgradeRequest) (WorkspaceInventory, error) {
	workspace, err := m.workspaceForUpgrade(req)
	if err != nil {
		return WorkspaceInventory{}, err
	}
	if m.InstanceActive(req.InstanceID) {
		return WorkspaceInventory{}, fmt.Errorf("instance %d still has an active gateway", req.InstanceID)
	}
	realRoot, err := filepath.EvalSymlinks(m.cfg.WorkspaceRoot)
	if err != nil {
		return WorkspaceInventory{}, fmt.Errorf("resolve workspace root: %w", err)
	}
	realWorkspace, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return WorkspaceInventory{}, fmt.Errorf("resolve instance workspace: %w", err)
	}
	if !pathWithin(realRoot, realWorkspace) {
		return WorkspaceInventory{}, ErrWorkspacePath
	}
	result := WorkspaceInventory{WorkspacePath: workspace, OpenClawHome: filepath.Join(workspace, "home", ".openclaw"), CheckedAt: time.Now().UTC()}
	result.AvailableBytes, _ = availableFilesystemBytes(workspace)
	// Inventory only OpenClaw's session migration inputs. Walking the complete
	// workspace made unrelated project *.db files part of the upgrade contract
	// and incorrectly required space for a full-workspace duplicate.
	err = filepath.WalkDir(result.OpenClawHome, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		rel, err := filepath.Rel(result.OpenClawHome, path)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return ErrWorkspacePath
		}
		if rel == "." {
			return nil
		}
		if entry.IsDir() && !sessionMigrationDirectory(filepath.ToSlash(rel)) {
			return filepath.SkipDir
		}
		if !entry.IsDir() && !sessionMigrationFile(filepath.ToSlash(rel)) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			result.DirectoryCount++
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			result.SymlinkCount++
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		result.FileCount++
		result.TotalBytes += info.Size()
		if isOpenClawSessionDatabase(filepath.ToSlash(rel), entry.Name()) {
			fact, err := m.preflightDatabase(ctx, workspace, path, info.Size(), req.OfficialDBCheck)
			if err != nil {
				return err
			}
			result.DatabaseFiles = append(result.DatabaseFiles, fact)
		}
		return nil
	})
	if err != nil {
		return WorkspaceInventory{}, err
	}
	sort.Slice(result.DatabaseFiles, func(i, j int) bool {
		return result.DatabaseFiles[i].RelativePath < result.DatabaseFiles[j].RelativePath
	})
	const migrationReserve = uint64(64 << 20)
	need := uint64(result.TotalBytes) + migrationReserve
	if result.AvailableBytes > 0 && need > result.AvailableBytes {
		return WorkspaceInventory{}, fmt.Errorf("insufficient session migration capacity: need at least %d bytes, available %d", need, result.AvailableBytes)
	}
	return result, nil
}

func sessionMigrationDirectory(rel string) bool {
	rel = strings.Trim(filepath.ToSlash(rel), "/")
	if rel == "" {
		return true
	}
	parts := strings.Split(rel, "/")
	if parts[0] == "sessions" || parts[0] == "session-sqlite-import-archive" || parts[0] == "session-sqlite-migration-runs" {
		return true
	}
	if parts[0] != "agents" {
		return false
	}
	if len(parts) <= 2 {
		return true
	}
	return parts[2] == "sessions" || parts[2] == "session-sqlite-import-archive" || parts[2] == "session-sqlite-migration-runs"
}

func sessionMigrationFile(rel string) bool {
	rel = strings.Trim(filepath.ToSlash(rel), "/")
	lower := strings.ToLower(rel)
	base := strings.ToLower(filepath.Base(rel))
	if base == "sessions.json" || base == "sessions.sqlite" || base == "sessions.sqlite3" || base == "sessions.db" {
		return true
	}
	if strings.Contains(lower, "session-sqlite-import-archive/") || strings.Contains(lower, "session-sqlite-migration-runs/") {
		return true
	}
	return strings.Contains(lower, "/sessions/") && (strings.HasSuffix(lower, ".jsonl") || strings.HasSuffix(lower, ".json"))
}

func isOpenClawSessionDatabase(rel, name string) bool {
	base := strings.ToLower(strings.TrimSpace(name))
	if base != "sessions.sqlite" && base != "sessions.sqlite3" && base != "sessions.db" {
		return false
	}
	return sessionMigrationFile(rel) && isSQLiteMainDatabase(base)
}

func isSQLiteMainDatabase(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	for _, suffix := range []string{".sqlite-wal", ".sqlite-shm", ".sqlite-journal", ".sqlite3-wal", ".sqlite3-shm", ".sqlite3-journal", ".db-wal", ".db-shm", ".db-journal"} {
		if strings.HasSuffix(lower, suffix) {
			return false
		}
	}
	for _, suffix := range []string{".lock.sqlite", ".lock.sqlite3", ".lock.db"} {
		if strings.HasSuffix(lower, suffix) {
			return false
		}
	}
	return strings.HasSuffix(lower, ".sqlite") || strings.HasSuffix(lower, ".sqlite3") || strings.HasSuffix(lower, ".db")
}

func (m *GatewayManager) preflightDatabase(ctx context.Context, workspace, source string, size int64, official bool) (DatabasePreflight, error) {
	rel, _ := filepath.Rel(workspace, source)
	fact := DatabasePreflight{RelativePath: filepath.ToSlash(rel), SizeBytes: size}
	file, err := os.Open(source)
	if err != nil {
		return fact, err
	}
	header := make([]byte, 16)
	n, readErr := io.ReadFull(file, header)
	_ = file.Close()
	if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) && !errors.Is(readErr, io.EOF) {
		return fact, readErr
	}
	fact.SQLiteHeader = n == 16 && string(header) == "SQLite format 3\x00"
	if !fact.SQLiteHeader {
		return fact, fmt.Errorf("database %s does not have a valid SQLite header", fact.RelativePath)
	}
	if !official {
		return fact, nil
	}
	tmpDir, err := os.MkdirTemp(m.upgradeRoot(), "db-preflight-")
	if err != nil {
		return fact, err
	}
	defer os.RemoveAll(tmpDir)
	copyPath := filepath.Join(tmpDir, "database.sqlite")
	if err := copyRegularFile(source, copyPath, 0o600); err != nil {
		return fact, err
	}
	command := exec.CommandContext(ctx, "openclaw", "database", "preflight", copyPath, "--json")
	output, err := command.CombinedOutput()
	if len(output) > 64*1024 {
		output = output[:64*1024]
	}
	digest := sha256.Sum256(output)
	fact.OutputSHA256 = hex.EncodeToString(digest[:])
	if err != nil {
		return fact, fmt.Errorf("OpenClaw database preflight failed for %s: %w", fact.RelativePath, err)
	}
	fact.OfficialOK = true
	return fact, nil
}

func (m *GatewayManager) upgradeRoot() string {
	return filepath.Join(m.cfg.WorkspaceRoot, ".clawmanager-upgrades")
}

func (m *GatewayManager) leasePath(instanceID int) string {
	return filepath.Join(m.upgradeRoot(), "leases", "instance-"+strconv.Itoa(instanceID)+".json")
}

func (m *GatewayManager) AcquireWriterLease(req WorkspaceUpgradeRequest, token string, ttl time.Duration) (WriterLease, error) {
	if _, err := m.workspaceForUpgrade(req); err != nil {
		return WriterLease{}, err
	}
	if err := validateUpgradeID("rollout_id", req.RolloutID); err != nil {
		return WriterLease{}, err
	}
	if err := validateUpgradeID("lease_token", token); err != nil {
		return WriterLease{}, err
	}
	if ttl < time.Minute || ttl > time.Hour {
		return WriterLease{}, errors.New("lease ttl must be between one minute and one hour")
	}
	if err := os.MkdirAll(filepath.Dir(m.leasePath(req.InstanceID)), 0o700); err != nil {
		return WriterLease{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.instanceActiveLocked(req.InstanceID) {
		return WriterLease{}, fmt.Errorf("instance %d still has an active gateway", req.InstanceID)
	}
	now := time.Now().UTC()
	lease := WriterLease{SchemaVersion: 1, RolloutID: req.RolloutID, InstanceID: req.InstanceID, UserID: req.UserID, Generation: req.Generation, Token: token, Holder: m.cfg.PodUID, ExpiresAt: now.Add(ttl), UpdatedAt: now}
	raw, _ := json.Marshal(lease)
	if existing, readErr := m.readWriterLease(req.InstanceID); readErr == nil {
		if existing.RolloutID == req.RolloutID && existing.InstanceID == req.InstanceID && existing.UserID == req.UserID && existing.Generation == req.Generation && existing.Token == token {
			if err := atomicWriteFile(m.leasePath(req.InstanceID), append(raw, '\n'), 0o600); err != nil {
				return WriterLease{}, err
			}
			return lease, nil
		}
		if existing.ExpiresAt.After(now) {
			return WriterLease{}, errors.New("writer lease is held by another upgrade operation")
		}
		if err := os.Remove(m.leasePath(req.InstanceID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return WriterLease{}, err
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return WriterLease{}, fmt.Errorf("read existing writer lease: %w", readErr)
	}
	file, err := os.OpenFile(m.leasePath(req.InstanceID), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return WriterLease{}, fmt.Errorf("writer lease unavailable: %w", err)
	}
	if _, err = file.Write(append(raw, '\n')); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return WriterLease{}, err
	}
	if closeErr != nil {
		return WriterLease{}, closeErr
	}
	return lease, nil
}

// writerLeaseBlocksGateway is called while the gateway manager mutex is held.
// A corrupt lease fails closed; an expired lease is removed so a crashed
// upgrade controller cannot permanently strand an instance.
func (m *GatewayManager) writerLeaseBlocksGateway(instanceID int) (bool, error) {
	lease, err := m.readWriterLease(instanceID)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read workspace writer lease: %w", err)
	}
	if lease.ExpiresAt.After(time.Now().UTC()) {
		return true, nil
	}
	if err := os.Remove(m.leasePath(instanceID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("remove expired workspace writer lease: %w", err)
	}
	return false, nil
}

func (m *GatewayManager) readWriterLease(instanceID int) (WriterLease, error) {
	var lease WriterLease
	raw, err := os.ReadFile(m.leasePath(instanceID))
	if err != nil {
		return lease, err
	}
	if err := json.Unmarshal(raw, &lease); err != nil {
		return lease, err
	}
	return lease, nil
}

func (m *GatewayManager) requireWriterLease(req WorkspaceUpgradeRequest) (WriterLease, error) {
	lease, err := m.readWriterLease(req.InstanceID)
	if err != nil {
		return lease, fmt.Errorf("read writer lease: %w", err)
	}
	if lease.RolloutID != req.RolloutID || lease.Token != req.LeaseToken || lease.Generation != req.Generation || lease.UserID != req.UserID {
		return lease, errors.New("writer lease identity mismatch")
	}
	if !lease.ExpiresAt.After(time.Now().UTC()) {
		return lease, errors.New("writer lease expired")
	}
	return lease, nil
}

func (m *GatewayManager) RenewWriterLease(req WorkspaceUpgradeRequest, ttl time.Duration) (WriterLease, error) {
	lease, err := m.requireWriterLease(req)
	if err != nil {
		return lease, err
	}
	if ttl < time.Minute || ttl > time.Hour {
		return lease, errors.New("lease ttl must be between one minute and one hour")
	}
	lease.UpdatedAt = time.Now().UTC()
	lease.ExpiresAt = lease.UpdatedAt.Add(ttl)
	raw, _ := json.Marshal(lease)
	if err := atomicWriteFile(m.leasePath(req.InstanceID), append(raw, '\n'), 0o600); err != nil {
		return WriterLease{}, err
	}
	return lease, nil
}

func (m *GatewayManager) ReleaseWriterLease(req WorkspaceUpgradeRequest) error {
	if _, err := m.requireWriterLease(req); err != nil {
		return err
	}
	return os.Remove(m.leasePath(req.InstanceID))
}

func (m *GatewayManager) CreateWorkspaceSnapshot(ctx context.Context, req WorkspaceUpgradeRequest) (SnapshotManifest, error) {
	return SnapshotManifest{}, errors.New("full workspace snapshots are disabled; use the OpenClaw session migration capsule")
}

func (m *GatewayManager) VerifyWorkspaceSnapshot(req WorkspaceUpgradeRequest) (SnapshotManifest, error) {
	return SnapshotManifest{}, errors.New("full workspace snapshots are disabled; use the OpenClaw session migration capsule")
}

type RestoreResult struct {
	WorkspacePath  string `json:"workspace_path"`
	PreservedPath  string `json:"preserved_path,omitempty"`
	SnapshotSHA256 string `json:"snapshot_sha256"`
}

func (m *GatewayManager) RestoreWorkspaceSnapshot(ctx context.Context, req WorkspaceUpgradeRequest) (RestoreResult, error) {
	return RestoreResult{}, errors.New("full workspace restore is disabled; use the OpenClaw session migration restore")
}

func copyRegularFile(source, target string, mode fs.FileMode) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func atomicWriteFile(path string, data []byte, mode fs.FileMode) error {
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp, path)
}
