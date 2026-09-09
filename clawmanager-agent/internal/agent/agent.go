package agent

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/iamlovingit/clawmanager-agent/internal/control"
	"github.com/iamlovingit/clawmanager-agent/internal/gateway"
)

type Agent struct {
	cfg      Config
	manager  *gateway.GatewayManager
	reporter *control.ReportClient

	podMu sync.RWMutex
	podID int
}

func NewAgent(cfg Config) *Agent {
	manager := gateway.NewGatewayManager(cfg, nil, nil)
	return &Agent{
		cfg:      cfg,
		manager:  manager,
		reporter: control.NewReportClient(cfg),
	}
}

func (a *Agent) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if a.manager.IsolatedGatewayLifecycle() {
		if err := a.manager.Initialize(); err != nil {
			return err
		}
		if err := a.manager.Health(); err != nil {
			return err
		}
		defer func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), a.cfg.ProcessStopTimeout+10*time.Second)
			defer stopCancel()
			_ = a.manager.Shutdown(stopCtx)
			_ = a.reporter.ReportGateways(stopCtx, a.manager.GatewayReportPayload(a.currentPodID()))
			_ = a.reporter.ReportHeartbeat(stopCtx, a.manager.HeartbeatPayload(a.currentPodID()))
		}()
	}

	listener, err := net.Listen("tcp", a.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen control server: %w", err)
	}
	server := &http.Server{
		Handler:           control.NewControlHandler(a.cfg, a.manager, a),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = server.Shutdown(shutdownCtx)
		cancel()
	}()

	if err := a.registerUntilReady(ctx); err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		a.heartbeatLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		a.metricsLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		a.gatewayReportLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		a.skillsLoop(ctx)
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		cancel()
		wg.Wait()
		return fmt.Errorf("control server: %w", err)
	}
	wg.Wait()
	return nil
}

func (a *Agent) ReportHeartbeat(ctx context.Context, payload gateway.HeartbeatPayload) error {
	if payload.PodID == 0 {
		payload.PodID = a.currentPodID()
	}
	return a.reporter.ReportHeartbeat(ctx, payload)
}

func (a *Agent) ReportSkills(ctx context.Context, payload gateway.SkillReportPayload) error {
	if payload.PodID == 0 {
		payload.PodID = a.currentPodID()
	}
	return a.reporter.ReportSkills(ctx, payload)
}

func (a *Agent) PodID() int {
	return a.currentPodID()
}

func (a *Agent) registerUntilReady(ctx context.Context) error {
	for {
		podID, err := a.reporter.Register(ctx, a.manager.RegisterPayload())
		if err == nil {
			a.setPodID(podID)
			return nil
		}
		log.Printf("runtime-agent register failed: %v", err)
		timer := time.NewTimer(a.cfg.RegisterRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (a *Agent) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer ticker.Stop()
	a.reportHeartbeat(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.reportHeartbeat(ctx)
		}
	}
}

func (a *Agent) reportHeartbeat(ctx context.Context) {
	if a.manager.IsolatedGatewayLifecycle() {
		_ = a.manager.RefreshClock(ctx)
	}
	if err := a.ReportHeartbeat(ctx, a.manager.HeartbeatPayload(a.currentPodID())); err != nil {
		log.Printf("runtime-agent heartbeat failed: %v", err)
	}
}

func (a *Agent) metricsLoop(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.MetricsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			payload := gateway.BuildMetricsPayload(a.cfg, a.manager, a.currentPodID())
			if err := a.reporter.ReportMetrics(ctx, payload); err != nil {
				log.Printf("runtime-agent metrics report failed: %v", err)
			}
		}
	}
}

func (a *Agent) gatewayReportLoop(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.GatewayReportInterval)
	defer ticker.Stop()
	changes := a.manager.GatewayStateChanges()
	a.reportGateways(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-changes:
			a.reportGateways(ctx)
		case <-ticker.C:
			a.reportGateways(ctx)
		}
	}
}

func (a *Agent) reportGateways(ctx context.Context) {
	payload := a.manager.GatewayReportPayload(a.currentPodID())
	if err := a.reporter.ReportGateways(ctx, payload); err != nil {
		log.Printf("runtime-agent gateways report failed: %v", err)
	}
}

func (a *Agent) skillsLoop(ctx context.Context) {
	if err := a.reporter.ReportSkills(ctx, gateway.BuildSkillReportPayload(a.cfg, a.manager, a.currentPodID(), "full")); err != nil {
		log.Printf("runtime-agent initial skills report failed: %v", err)
	}
	ticker := time.NewTicker(a.cfg.SkillsReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.reporter.ReportSkills(ctx, gateway.BuildSkillReportPayload(a.cfg, a.manager, a.currentPodID(), "full")); err != nil {
				log.Printf("runtime-agent skills report failed: %v", err)
			}
		}
	}
}

func (a *Agent) setPodID(podID int) {
	a.podMu.Lock()
	defer a.podMu.Unlock()
	a.podID = podID
}

func (a *Agent) currentPodID() int {
	a.podMu.RLock()
	defer a.podMu.RUnlock()
	return a.podID
}
