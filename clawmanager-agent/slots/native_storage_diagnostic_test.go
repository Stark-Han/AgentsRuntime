package slots

import (
	"errors"
	"strings"
	"testing"
)

func TestNativeStorageDiagnosticDoesNotExposeOutputOrCredentials(t *testing.T) {
	for _, tc := range []struct{ text, code string }{{"bwrap: private-secret /secret/path", "bubblewrap"}, {"permission denied token=private-secret", "permission_denied"}, {"native helper identity mismatch", "helper_identity"}, {"private-secret unknown configuration", "helper_failed"}, {"native_storage_config: permission denied token=private-secret", "config_permission_denied"}} {
		got := nativeStorageFailureCategory([]byte(tc.text), errors.New("exit status 1"))
		if got != tc.code || strings.Contains(got, "private-secret") || strings.Contains(got, "/secret") {
			t.Fatalf("unsafe diagnostic %q", got)
		}
	}
}
