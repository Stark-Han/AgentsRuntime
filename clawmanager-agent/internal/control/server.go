package control

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/iamlovingit/clawmanager-agent/internal/gateway"
)

const ControlTokenHeader = gateway.ControlTokenHeader

func NewControlHandler(cfg gateway.Config, manager *gateway.GatewayManager, reporter gateway.HeartbeatReporter) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/health", func(w http.ResponseWriter, r *http.Request) {
		if !authorizeControl(cfg, w, r) {
			return
		}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := manager.Health(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		status := "ready"
		if manager.Draining() {
			status = "draining"
		}
		writeJSON(w, http.StatusOK, gateway.HealthResponse{Status: status, Capabilities: manager.HealthCapabilities()})
	})

	mux.HandleFunc("/v1/gateways", func(w http.ResponseWriter, r *http.Request) {
		if !authorizeControl(cfg, w, r) {
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req gateway.CreateGatewayRequest
		body := r.Body
		if manager.IsolatedGatewayLifecycle() {
			body = http.MaxBytesReader(w, r.Body, 1<<20)
		}
		decoder := json.NewDecoder(body)
		if manager.IsolatedGatewayLifecycle() {
			decoder.DisallowUnknownFields()
		}
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if manager.IsolatedGatewayLifecycle() {
			var extra any
			if err := decoder.Decode(&extra); err != io.EOF {
				http.Error(w, "invalid json", http.StatusBadRequest)
				return
			}
		}
		resp, err := manager.CreateGateway(r.Context(), req)
		if err != nil {
			writeCreateGatewayError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})

	mux.HandleFunc("/v1/gateways/", func(w http.ResponseWriter, r *http.Request) {
		if !authorizeControl(cfg, w, r) {
			return
		}
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		escapedID := strings.TrimPrefix(r.URL.Path, "/v1/gateways/")
		gatewayID, err := url.PathUnescape(escapedID)
		if err != nil {
			http.Error(w, "invalid gateway id", http.StatusBadRequest)
			return
		}
		if err := manager.DeleteGateway(r.Context(), gatewayID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("/v1/drain", func(w http.ResponseWriter, r *http.Request) {
		if !authorizeControl(cfg, w, r) {
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Draining bool `json:"draining"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && r.ContentLength != 0 {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		manager.SetDraining(req.Draining)
		if reporter != nil {
			_ = reporter.ReportHeartbeat(context.Background(), manager.HeartbeatPayload(0))
		}
		writeJSON(w, http.StatusOK, map[string]bool{"draining": manager.Draining()})
	})

	mux.HandleFunc("/v1/skills/resync", func(w http.ResponseWriter, r *http.Request) {
		if !authorizeControl(cfg, w, r) {
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			InstanceID int    `json:"instance_id"`
			Mode       string `json:"mode"`
			Trigger    string `json:"trigger"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && r.ContentLength != 0 {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		skillsReporter, ok := reporter.(gateway.SkillsReporter)
		if !ok || skillsReporter == nil {
			http.Error(w, "skills reporter unavailable", http.StatusServiceUnavailable)
			return
		}
		mode := strings.TrimSpace(req.Mode)
		if mode == "" {
			mode = "full"
		}
		payload := gateway.BuildSkillReportPayloadForInstance(cfg, manager, skillsReporter.PodID(), mode, req.InstanceID)
		if err := skillsReporter.ReportSkills(r.Context(), payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"ok":          true,
			"mode":        mode,
			"trigger":     strings.TrimSpace(req.Trigger),
			"instance_id": req.InstanceID,
			"instances":   len(payload.Instances),
		})
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(noRedirectResponseWriter{ResponseWriter: w}, r)
	})
}

type noRedirectResponseWriter struct {
	http.ResponseWriter
}

func (w noRedirectResponseWriter) WriteHeader(code int) {
	if code >= 300 && code < 400 {
		code = http.StatusNotFound
	}
	w.ResponseWriter.WriteHeader(code)
}

func authorizeControl(cfg gateway.Config, w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get(ControlTokenHeader) != cfg.ControlToken {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func writeCreateGatewayError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, gateway.ErrRuntimeType), errors.Is(err, gateway.ErrWorkspacePath):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, gateway.ErrDraining), errors.Is(err, gateway.ErrNoFreePort), errors.Is(err, gateway.ErrStaleGeneration):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, gateway.ErrGatewayStartFailed):
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
