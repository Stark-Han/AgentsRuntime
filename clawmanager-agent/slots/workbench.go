package slots

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/iamlovingit/clawmanager-agent/gateway"
	"github.com/iamlovingit/clawmanager-agent/workbench"
	"github.com/iamlovingit/clawmanager-agent/workspacegit"
)

const workbenchSlotBudget = 2 * time.Second

var errWorkbenchSlotUnavailable = errors.New("workbench slot state is temporarily unavailable")

func decodeWorkbench(r *http.Request) (workbench.Request, []byte, error) {
	var req workbench.Request
	if r.Method != "POST" {
		return req, nil, workbench.Invalid()
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, workbench.MaxRequest+1))
	if err != nil || len(raw) > workbench.MaxRequest || json.Unmarshal(raw, &req) != nil || req.InstanceID <= 0 || req.UserID <= 0 || req.Generation <= 0 || req.GatewayID == "" || req.Operation == "" {
		return req, nil, workbench.Invalid()
	}
	return req, raw, nil
}
func workbenchError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	safe := &workbench.Error{Code: "execution_failed", Message: "Business operation failed."}
	var e *workbench.Error
	var git *workspacegit.Error
	switch {
	case errors.Is(err, gateway.ErrStaleGeneration):
		status = 409
		safe = &workbench.Error{Code: "stale_generation", Message: "Instance assignment has changed."}
	case errors.Is(err, errWorkbenchSlotUnavailable):
		status = http.StatusServiceUnavailable
		safe = &workbench.Error{Code: "slot_state_unavailable", Message: "Runtime slot state is temporarily unavailable."}
	case errors.Is(err, gateway.ErrGatewayIdleBusy):
		status = 423
		safe = &workbench.Error{Code: "instance_busy", Message: "Instance is stopping or unavailable."}
	case errors.Is(err, gateway.ErrGatewayStopFailed):
		status = 423
		safe = &workbench.Error{Code: "stop_unconfirmed", Message: "Business process teardown is not confirmed."}
	case errors.Is(err, context.DeadlineExceeded):
		status = 504
		safe = &workbench.Error{Code: "timeout", Message: "Operation timed out."}
	case errors.Is(err, context.Canceled):
		status = 409
		safe = &workbench.Error{Code: "cancelled", Message: "Operation was cancelled."}
	case errors.As(err, &git):
		status = 400
		safe = &workbench.Error{Code: git.Code, Message: git.Message}
	case errors.As(err, &e):
		status = 400
		safe = e
	}
	writeJSON(w, status, map[string]any{"error": safe})
}
func matchesWorkbench(a *Assignment, req workbench.Request) bool {
	return a != nil && a.InstanceID == req.InstanceID && a.UserID == req.UserID && a.Generation == req.Generation && a.GatewayID == req.GatewayID && a.State == "starting" && !a.IdleStop
}
func (p *Pool) serveWorkbench(w http.ResponseWriter, r *http.Request) {
	req, raw, err := decodeWorkbench(r)
	if err != nil {
		workbenchError(w, err)
		return
	}
	selected, err := p.workbenchSlot(r.Context(), req)
	if err != nil {
		workbenchError(w, err)
		return
	}
	p.forward(w, r, selected, raw)
}
func (w *Worker) serveWorkbench(out http.ResponseWriter, r *http.Request) {
	req, _, err := decodeWorkbench(r)
	if err != nil {
		workbenchError(out, err)
		return
	}
	if err = w.lockWorkbenchSlot(r.Context(), req); err != nil {
		workbenchError(out, err)
		return
	}
	result, err := w.manager.WorkbenchWithAdmission(r.Context(), req, w.mu.Unlock)
	if err != nil {
		workbenchError(out, err)
		return
	}
	writeJSON(out, 200, result)
}

// On success the caller holds the assignment fence through manager admission.
func (w *Worker) lockWorkbenchSlot(ctx context.Context, req workbench.Request) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !w.mu.TryLock() {
		return errWorkbenchSlotUnavailable
	}
	if w.book == nil || w.book.quarantined || w.book.StopIntent != nil || w.book.Maintenance != nil || !matchesWorkbench(w.book.Assignment, req) {
		w.mu.Unlock()
		return gateway.ErrStaleGeneration
	}
	if w.storageHealth != nil && w.manager.Health() != nil {
		w.mu.Unlock()
		return errWorkbenchSlotUnavailable
	}
	return nil
}

func (p *Pool) workbenchSlot(ctx context.Context, req workbench.Request) (int, error) {
	p.mu.RLock()
	draining := p.draining
	p.mu.RUnlock()
	if draining {
		return -1, gateway.ErrGatewayIdleBusy
	}
	probeCtx, cancel := context.WithTimeout(ctx, workbenchSlotBudget)
	defer cancel()
	snapshots, healthy := p.snapshots(probeCtx)
	selected := -1
	matches := 0
	// Unknown slots cannot prove that the requested identity has a unique owner.
	if healthy != p.count {
		for _, snapshot := range snapshots {
			if snapshot.unreachable || snapshot.StorageUnavailable {
				return -1, errWorkbenchSlotUnavailable
			}
		}
		return -1, gateway.ErrStaleGeneration
	}
	for i, snapshot := range snapshots {
		if a := snapshot.Assignment; a != nil && a.InstanceID == req.InstanceID && a.State != "stopped" {
			matches++
			if !snapshot.Quarantined && matchesWorkbench(a, req) {
				for _, g := range snapshot.Gateways {
					if g.GatewayID == req.GatewayID && g.InstanceID == req.InstanceID && g.UserID == req.UserID && g.Generation == req.Generation && g.State == "running" {
						selected = i
					}
				}
			}
		}
	}
	if selected < 0 || matches != 1 {
		return -1, gateway.ErrStaleGeneration
	}
	return selected, nil
}
