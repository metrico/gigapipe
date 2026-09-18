package envalias

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// TestGenericPrefixIsMapped covers the rule that carries every viper-backed
// config key: GIGAPIPE_X has to reach cloki-config, which only ever looks at
// QRYN_X.
func TestGenericPrefixIsMapped(t *testing.T) {
	t.Setenv("GIGAPIPE_SYSTEM_SETTINGS_OTLP_MAX_MESSAGE_SIZE", "1234")
	Apply()
	if got := os.Getenv("QRYN_SYSTEM_SETTINGS_OTLP_MAX_MESSAGE_SIZE"); got != "1234" {
		t.Errorf("QRYN_SYSTEM_SETTINGS_OTLP_MAX_MESSAGE_SIZE = %q, want %q", got, "1234")
	}
}

// TestCredentialsFeedBothLegacyPrefixes guards the reason credentials are
// special-cased: cmd reads QRYN_LOGIN and then lets CLOKI_LOGIN override it, so
// writing only QRYN_LOGIN would let a stale CLOKI_LOGIN win over the value the
// operator actually set.
func TestCredentialsFeedBothLegacyPrefixes(t *testing.T) {
	t.Setenv("CLOKI_LOGIN", "stale")
	t.Setenv("GIGAPIPE_LOGIN", "current")
	Apply()
	for _, name := range []string{"QRYN_LOGIN", "CLOKI_LOGIN"} {
		if got := os.Getenv(name); got != "current" {
			t.Errorf("%s = %q, want %q", name, got, "current")
		}
	}
}

// TestIrregularNamesAreMapped covers the legacy names that have no underscore
// after their prefix, which the generic rule cannot derive.
func TestIrregularNamesAreMapped(t *testing.T) {
	t.Setenv("GIGAPIPE_APPLOGPATH", "/var/log")
	t.Setenv("GIGAPIPE_APPLOGNAME", "writer.log")
	Apply()
	if got := os.Getenv("CLOKIAPPLOGPATH"); got != "/var/log" {
		t.Errorf("CLOKIAPPLOGPATH = %q, want %q", got, "/var/log")
	}
	if got := os.Getenv("CLOKIAPPLOGNAME"); got != "writer.log" {
		t.Errorf("CLOKIAPPLOGNAME = %q, want %q", got, "writer.log")
	}
}

// TestNewNameWinsOverLegacy pins the precedence rule: setting both is not an
// error, and the GIGAPIPE_ value is the one that takes effect.
func TestNewNameWinsOverLegacy(t *testing.T) {
	t.Setenv("QRYN_RULER_ENABLED", "false")
	t.Setenv("GIGAPIPE_RULER_ENABLED", "true")
	Apply()
	if got := os.Getenv("QRYN_RULER_ENABLED"); got != "true" {
		t.Errorf("QRYN_RULER_ENABLED = %q, want %q", got, "true")
	}
}

// TestLegacyOnlyDeploymentIsUntouched is the compatibility guarantee: an
// existing deployment that knows nothing about GIGAPIPE_ must behave exactly as
// it did before.
func TestLegacyOnlyDeploymentIsUntouched(t *testing.T) {
	t.Setenv("QRYN_LOGIN", "legacy")
	t.Setenv("QRYN_PASSWORD", "secret")
	Apply()
	if got := os.Getenv("QRYN_LOGIN"); got != "legacy" {
		t.Errorf("QRYN_LOGIN = %q, want %q", got, "legacy")
	}
	if got := os.Getenv("QRYN_PASSWORD"); got != "secret" {
		t.Errorf("QRYN_PASSWORD = %q, want %q", got, "secret")
	}
	if _, set := os.LookupEnv("CLOKI_LOGIN"); set {
		t.Error("CLOKI_LOGIN was created for a deployment that never set a GIGAPIPE_ variable")
	}
}

func TestDeprecationWarningNamesTheReplacement(t *testing.T) {
	t.Setenv("QRYN_RULER_POLL_INTERVAL", "30s")
	warnings := Apply()
	idx := slices.IndexFunc(warnings, func(w string) bool {
		return strings.Contains(w, "QRYN_RULER_POLL_INTERVAL")
	})
	if idx < 0 {
		t.Fatalf("no warning for QRYN_RULER_POLL_INTERVAL, got %v", warnings)
	}
	if !strings.Contains(warnings[idx], "GIGAPIPE_RULER_POLL_INTERVAL") {
		t.Errorf("warning does not name the replacement: %q", warnings[idx])
	}
}

// TestNoWarningWhenAlreadyMigrated keeps the warning from firing at every
// start-up for a deployment that has already moved: Apply writes the legacy
// name itself, so a naive check would always see it set.
func TestNoWarningWhenAlreadyMigrated(t *testing.T) {
	t.Setenv("GIGAPIPE_RULER_ENABLED", "true")
	for _, w := range Apply() {
		if strings.Contains(w, "QRYN_RULER_ENABLED") {
			t.Errorf("warned about a variable the deployment did not set: %q", w)
		}
	}
}
