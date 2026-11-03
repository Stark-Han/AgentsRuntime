package opencode

import (
	"encoding/json"
	"testing"

	"github.com/iamlovingit/clawmanager-agent/gateway"
	"github.com/iamlovingit/clawmanager-agent/llmconfig"
)

func TestV2GatewayPrefersRealGatewayModelNames(t *testing.T) {
	t.Setenv("OPENCODE_RUNTIME_MAJOR", "2")
	settings, err := resolveLLMSettings(gateway.Config{}, gateway.CreateGatewayRequest{Environment: map[string]string{
		"CLAWMANAGER_LLM_BASE_URL": "https://gateway.example/v1", "CLAWMANAGER_LLM_API_KEY": "test",
		"CLAWMANAGER_LLM_MODEL": `["deployment-name"]`, "CLAWMANAGER_LLM_PROVIDER_MODELS": `["provider/different-name"]`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := RenderConfig(settings)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err = json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	models := doc["provider"].(map[string]any)["clawmanager"].(map[string]any)["models"].(map[string]any)
	if models["deployment-name"] == nil || models["different-name"] != nil {
		t.Fatalf("gateway names lost: %v", models)
	}
}

func TestV2ConfigKeepsManagedProviderAndModalities(t *testing.T) {
	t.Setenv("OPENCODE_RUNTIME_MAJOR", "2")
	raw, err := RenderConfig(llmconfig.Settings{BaseURL: "https://gateway.example/v1", APIKey: "test", APIKeySet: true, ModelIDs: []string{"openai/auto"}, ModelsQualified: true, Vision: map[string]bool{"auto": true}})
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err = json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["model"] != "clawmanager/auto" {
		t.Fatalf("model = %v", doc["model"])
	}
	providers := doc["provider"].(map[string]any)
	if len(providers) != 1 {
		t.Fatalf("unexpected providers: %v", providers)
	}
	model := providers["clawmanager"].(map[string]any)["models"].(map[string]any)["auto"].(map[string]any)
	if model["modalities"] == nil || model["capabilities"] != nil {
		t.Fatalf("invalid v2 model: %v", model)
	}
}

func TestLegacyConfigRetainsQualifiedProvider(t *testing.T) {
	t.Setenv("OPENCODE_RUNTIME_MAJOR", "")
	t.Setenv("CLAWMANAGER_MODEL_GOVERNANCE_PROTOCOL", "")
	raw, err := RenderConfig(llmconfig.Settings{BaseURL: "https://gateway.example/v1", APIKey: "test", APIKeySet: true, ModelIDs: []string{"openai/model"}, ModelsQualified: true})
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err = json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["model"] != "openai/model" {
		t.Fatalf("legacy model changed: %v", doc["model"])
	}
}
