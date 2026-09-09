package gateway

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type isolatedProcessLog struct {
	once                   sync.Once
	instanceID, generation int
}

func (w *isolatedProcessLog) Write(data []byte) (int, error) {
	w.once.Do(func() {
		log.Printf("runtime-agent backend output received: instance_id=%d generation=%d category=backend_output_redacted", w.instanceID, w.generation)
	})
	for _, category := range []string{"untrusted_gateway_peer", "origin_required", "origin_rejected", "websocket_origin_rejected", "unsupported_native_desktop_api"} {
		if strings.Contains(string(data), "hermes_lite_request_denied reason="+category) {
			log.Printf("runtime-agent request denied: instance_id=%d generation=%d category=%s", w.instanceID, w.generation, category)
		}
	}
	return len(data), nil
}

// Opt-in lifecycle for the Hermes Desktop Web Lite image. Older runtime
// profiles retain their existing contract until separately migrated.
func (m *GatewayManager) IsolatedGatewayLifecycle() bool {
	p, ok := m.profile().(interface{ IsolatedGatewayLifecycle() bool })
	return ok && p.IsolatedGatewayLifecycle()
}

func (m *GatewayManager) createIsolatedGateway(req CreateGatewayRequest) (CreateGatewayResponse, error) {
	if req.AgentType != m.cfg.RuntimeType {
		return CreateGatewayResponse{}, ErrRuntimeType
	}
	workspace, err := ValidateWorkspacePath(m.cfg.WorkspaceRoot, m.cfg.RuntimeType, req)
	if err != nil {
		return CreateGatewayResponse{}, ErrWorkspacePath
	}
	if req.GatewayPort < 0 || (req.GatewayPort != 0 && (req.GatewayPort < m.cfg.GatewayPortStart || req.GatewayPort > m.cfg.GatewayPortEnd)) {
		return CreateGatewayResponse{}, fmt.Errorf("port_conflict: assigned port outside configured pool")
	}
	rng := req.PortRange
	if rng.Start == 0 && rng.End == 0 {
		rng = PortRange{Start: m.cfg.GatewayPortStart, End: m.cfg.GatewayPortEnd}
	}
	if rng.Start < m.cfg.GatewayPortStart || rng.End > m.cfg.GatewayPortEnd || rng.End < rng.Start || rng.Start < 1 || rng.End > 65535 {
		return CreateGatewayResponse{}, fmt.Errorf("port_conflict: invalid port range")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.draining {
		return CreateGatewayResponse{}, ErrDraining
	}
	if m.clockIssue != "" {
		return CreateGatewayResponse{}, fmt.Errorf("%s", m.clockIssue)
	}
	id := gatewayID(req.InstanceID, req.Generation)
	if existing := m.gateways[id]; existing != nil {
		return createGatewayResponse(existing.state), nil
	}
	var previous []*gatewayRecord
	replacing := 0
	for _, record := range m.gateways {
		if record.state.InstanceID == req.InstanceID {
			if record.state.Generation > req.Generation {
				return CreateGatewayResponse{}, ErrStaleGeneration
			}
			previous = append(previous, record)
			if record.state.State == "starting" || record.state.State == "running" {
				replacing++
			}
		} else if req.GatewayPort > 0 && record.state.Port == req.GatewayPort && !recordFinished(record) {
			return CreateGatewayResponse{}, fmt.Errorf("port_conflict: %w", ErrNoFreePort)
		}
	}
	if m.usedSlotsLocked()-replacing >= m.effectiveCapacityLocked() {
		return CreateGatewayResponse{}, ErrNoFreePort
	}
	port, reserved := req.GatewayPort, false
	if port == 0 {
		port, err = m.ports.Reserve(req.InstanceID, req.Generation, rng)
		reserved = err == nil
	} else {
		deferred := false
		for _, old := range previous {
			if old.state.Port == port && !recordFinished(old) {
				deferred = true
			}
		}
		if !deferred {
			port, err = m.ports.ReserveExact(req.InstanceID, req.Generation, port)
			reserved = err == nil
		}
	}
	if err != nil {
		return CreateGatewayResponse{}, fmt.Errorf("port_conflict: %w", ErrNoFreePort)
	}
	ctx, cancel := context.WithCancel(context.Background())
	now := time.Now().UTC()
	record := &gatewayRecord{cancel: cancel, finished: make(chan struct{}), reserved: reserved, state: GatewayState{
		InstanceID: req.InstanceID, UserID: req.UserID, GatewayID: id, RuntimeType: m.cfg.RuntimeType,
		WorkspacePath: workspace, Port: port, PortAlias: port, UID: req.UID, GID: req.GID,
		CPUCores: req.CPUCores, MemoryMB: req.MemoryMB, DiskQuotaMB: req.DiskQuotaMB,
		Generation: req.Generation, State: "starting", StartedAt: now, UpdatedAt: now,
	}}
	for _, old := range previous {
		if old.cancel != nil {
			old.cancel()
		}
	}
	m.gateways[id] = record
	m.notifyGatewayStateChangedLocked()
	go m.runIsolatedGateway(ctx, record, previous, req)
	return createGatewayResponse(record.state), nil
}

func recordFinished(record *gatewayRecord) bool {
	if record.finished == nil {
		return true
	}
	select {
	case <-record.finished:
		return true
	default:
		return false
	}
}

func (m *GatewayManager) runIsolatedGateway(ctx context.Context, record *gatewayRecord, previous []*gatewayRecord, req CreateGatewayRequest) {
	var process ManagedProcess
	var failure error
	defer func() {
		stopFailed := false
		// Stop must finish before any reservation is released or successor can
		// write the same workspace. It includes the entire process group.
		if process.Stop != nil {
			stopCtx, cancel := context.WithTimeout(context.Background(), m.stopTimeout()+5*time.Second)
			if err := process.Stop(stopCtx); err != nil {
				failure = fmt.Errorf("gateway_stop_failed")
				stopFailed = true
			}
			cancel()
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if record.reserved && !stopFailed {
			m.ports.Release(record.state.Port)
			record.reserved = false
		}
		if !stopFailed {
			record.process = ManagedProcess{}
			record.state.PID = 0
		}
		record.state.State = "stopped"
		record.state.ErrorMessage = ""
		if failure != nil && (ctx.Err() == nil || stopFailed) {
			record.state.State = "error"
			record.state.ErrorMessage = safeGatewayError(failure)
		}
		record.state.UpdatedAt = time.Now().UTC()
		record.state.HealthAt = record.state.UpdatedAt
		close(record.finished)
		m.notifyGatewayStateChangedLocked()
		log.Printf("runtime-agent gateway completed: instance_id=%d generation=%d state=%s error=%s", req.InstanceID, req.Generation, record.state.State, record.state.ErrorMessage)
	}()
	for _, old := range previous {
		if old.finished != nil {
			select {
			case <-old.finished:
			case <-ctx.Done():
				return
			}
		}
		m.mu.RLock()
		stopFailed := old.reserved
		m.mu.RUnlock()
		if stopFailed {
			failure = fmt.Errorf("gateway_stop_failed")
			return
		}
		m.mu.Lock()
		if m.gateways[old.state.GatewayID] == old {
			delete(m.gateways, old.state.GatewayID)
		}
		m.mu.Unlock()
	}
	if ctx.Err() != nil {
		return
	}
	m.mu.Lock()
	if !record.reserved {
		_, failure = m.ports.ReserveExact(req.InstanceID, req.Generation, record.state.Port)
		record.reserved = failure == nil
	}
	m.mu.Unlock()
	if failure != nil {
		failure = fmt.Errorf("port_conflict")
		return
	}
	workspace := record.state.WorkspacePath
	if failure = m.profile().PrepareWorkspace(m.cfg, req, workspace); failure != nil {
		return
	}
	if ctx.Err() != nil {
		return
	}
	if failure = m.profile().WriteGatewayConfig(m.cfg, req, workspace, record.state.Port); failure != nil {
		return
	}
	if ctx.Err() != nil {
		return
	}
	spec := GatewayStartSpec{GatewayID: record.state.GatewayID, RuntimeType: m.cfg.RuntimeType,
		InstanceID: req.InstanceID, UserID: req.UserID, WorkspacePath: workspace, Port: record.state.Port,
		UID: req.UID, GID: req.GID, Generation: req.Generation, CPUCores: req.CPUCores, MemoryMB: req.MemoryMB, DiskQuotaMB: req.DiskQuotaMB,
		Command: m.profile().GatewayCommand(m.cfg.GatewayAuthMode), Env: m.profile().GatewayEnv(os.Environ(), m.cfg, req, workspace, record.state.Port)}
	process, failure = m.starter.StartGateway(ctx, spec)
	m.mu.Lock()
	record.process, record.state.PID = process, process.PID
	m.mu.Unlock()
	if failure != nil {
		return
	}
	healthCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ready := make(chan error, 1)
	go func() { ready <- m.health.WaitReady(healthCtx, spec) }()
	select {
	case <-ctx.Done():
		return
	case <-process.Done:
		failure = fmt.Errorf("gateway_process_exited")
		return
	case failure = <-ready:
		if failure != nil {
			return
		}
	}
	select {
	case <-process.Done:
		failure = fmt.Errorf("gateway_process_exited")
		return
	default:
	}
	if ctx.Err() != nil {
		return
	}
	if verifier, ok := m.starter.(interface {
		VerifyListener(GatewayStartSpec, int) error
	}); ok {
		if failure = verifier.VerifyListener(spec, process.PID); failure != nil {
			return
		}
	}
	if failure = m.Health(); failure != nil {
		return
	}
	m.mu.Lock()
	m.ports.Commit(req.InstanceID, req.Generation, spec.Port)
	record.state.State, record.state.HealthAt, record.state.UpdatedAt = "running", time.Now().UTC(), time.Now().UTC()
	m.notifyGatewayStateChangedLocked()
	m.mu.Unlock()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-process.Done:
			failure = fmt.Errorf("gateway_process_exited")
			return
		case <-ticker.C:
			if failure = m.Health(); failure != nil && failure.Error() != "clock_reference_unavailable" {
				return
			}
			checker, ok := m.health.(interface {
				CheckHealth(context.Context, GatewayStartSpec) error
			})
			if !ok {
				continue
			}
			probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
			failure = checker.CheckHealth(probeCtx, spec)
			if failure == nil {
				if verifier, ok := m.starter.(interface {
					VerifyListener(GatewayStartSpec, int) error
				}); ok {
					failure = verifier.VerifyListener(spec, process.PID)
				}
			}
			probeCancel()
			m.mu.Lock()
			record.state.HealthAt = time.Now().UTC()
			if failure != nil {
				record.state.State = "unhealthy"
				record.state.ErrorMessage = safeGatewayError(failure)
				m.notifyGatewayStateChangedLocked()
			}
			m.mu.Unlock()
			if failure != nil {
				return
			}
		}
	}
}

func safeGatewayError(err error) string {
	if strings.HasPrefix(err.Error(), "clock_skew") {
		return "clock_skew"
	}
	if strings.HasPrefix(err.Error(), "clock_reference_unavailable") {
		return "clock_reference_unavailable"
	}
	for _, code := range []string{"missing_llm_credentials", "dashboard_auth_failed", "websocket_origin_rejected", "unsupported_hermes_protocol", "workspace_unavailable", "port_conflict", "invalid_gateway_identity", "gateway_process_exited", "gateway_stop_failed", "http_health_failed", "websocket_handshake_failed", "incompatible_workspace_version", "dashboard_not_ready", "dashboard_http_unavailable", "websocket_unavailable", "websocket_upgrade_rejected", "workspace_unusable", "invalid_control_ui_origin", "invalid_trusted_proxies"} {
		if strings.HasPrefix(err.Error(), code) {
			return code
		}
	}
	return "gateway_start_failed"
}

func (m *GatewayManager) deleteIsolatedGateway(ctx context.Context, id string) error {
	m.mu.Lock()
	record := m.gateways[id]
	if record == nil {
		m.mu.Unlock()
		return nil
	}
	record.cancel()
	m.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-record.finished:
	}
	record.stopMu.Lock()
	defer record.stopMu.Unlock()
	m.mu.RLock()
	reserved, process := record.reserved, record.process
	m.mu.RUnlock()
	if reserved && process.Stop != nil {
		if err := process.Stop(ctx); err != nil {
			return fmt.Errorf("gateway_stop_failed")
		}
		m.mu.Lock()
		m.ports.Release(record.state.Port)
		record.reserved = false
		m.mu.Unlock()
	}
	m.mu.Lock()
	if record.reserved {
		m.mu.Unlock()
		return fmt.Errorf("gateway_stop_failed")
	}
	if m.gateways[id] == record {
		delete(m.gateways, id)
		m.notifyGatewayStateChangedLocked()
	}
	m.mu.Unlock()
	return nil
}

func (m *GatewayManager) Initialize() error {
	if !m.IsolatedGatewayLifecycle() {
		return nil
	}
	if err := recoverIsolatedProcesses(m.cfg); err != nil {
		return err
	}
	return m.RefreshClock(context.Background())
}

func (m *GatewayManager) Shutdown(ctx context.Context) error {
	m.SetDraining(true)
	if !m.IsolatedGatewayLifecycle() {
		return nil
	}
	m.mu.Lock()
	var records []*gatewayRecord
	for _, record := range m.gateways {
		record.cancel()
		records = append(records, record)
	}
	m.mu.Unlock()
	for _, record := range records {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-record.finished:
		}
		m.mu.RLock()
		reserved := record.reserved
		m.mu.RUnlock()
		if reserved {
			return fmt.Errorf("gateway_stop_failed")
		}
	}
	return nil
}
