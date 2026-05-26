package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSetDefaults_BasicStruct(t *testing.T) {
	type testStruct struct {
		Name    string `default:"hello"`
		Port    int    `default:"8080"`
		Enabled bool   `default:"true"`
	}

	s := &testStruct{}
	if err := SetDefaults(s, "default"); err != nil {
		t.Fatalf("SetDefaults failed: %v", err)
	}

	if s.Name != "hello" {
		t.Errorf("Name = %q, want %q", s.Name, "hello")
	}
	if s.Port != 8080 {
		t.Errorf("Port = %d, want %d", s.Port, 8080)
	}
	if s.Enabled != true {
		t.Errorf("Enabled = %v, want %v", s.Enabled, true)
	}
}

func TestSetDefaults_PreserveExisting(t *testing.T) {
	type testStruct struct {
		Name string `default:"hello"`
		Port int    `default:"8080"`
	}

	s := &testStruct{Name: "custom", Port: 9090}
	if err := SetDefaults(s, "default"); err != nil {
		t.Fatalf("SetDefaults failed: %v", err)
	}

	if s.Name != "custom" {
		t.Errorf("Name = %q, want %q (should preserve existing)", s.Name, "custom")
	}
	if s.Port != 9090 {
		t.Errorf("Port = %d, want %d (should preserve existing)", s.Port, 9090)
	}
}

func TestSetDefaults_NestedStruct(t *testing.T) {
	type Inner struct {
		Value string `default:"inner_default"`
	}
	type Outer struct {
		Inner Inner
		Name  string `default:"outer_default"`
	}

	s := &Outer{}
	if err := SetDefaults(s, "default"); err != nil {
		t.Fatalf("SetDefaults failed: %v", err)
	}

	if s.Name != "outer_default" {
		t.Errorf("Name = %q, want %q", s.Name, "outer_default")
	}
	if s.Inner.Value != "inner_default" {
		t.Errorf("Inner.Value = %q, want %q", s.Inner.Value, "inner_default")
	}
}

func TestSetDefaults_UintAndFloat(t *testing.T) {
	type testStruct struct {
		UintVal  uint    `default:"42"`
		FloatVal float64 `default:"3.14"`
	}

	s := &testStruct{}
	if err := SetDefaults(s, "default"); err != nil {
		t.Fatalf("SetDefaults failed: %v", err)
	}

	if s.UintVal != 42 {
		t.Errorf("UintVal = %d, want %d", s.UintVal, 42)
	}
	if s.FloatVal != 3.14 {
		t.Errorf("FloatVal = %f, want %f", s.FloatVal, 3.14)
	}
}

func TestSetDefaults_SliceOfStructs(t *testing.T) {
	type Item struct {
		Name string `default:"default_item"`
	}
	type testStruct struct {
		Items []Item
	}

	s := &testStruct{}
	if err := SetDefaults(s, "default"); err != nil {
		t.Fatalf("SetDefaults failed: %v", err)
	}

	if len(s.Items) != 1 {
		t.Fatalf("Items length = %d, want 1", len(s.Items))
	}
	if s.Items[0].Name != "default_item" {
		t.Errorf("Items[0].Name = %q, want %q", s.Items[0].Name, "default_item")
	}
}

func TestSetDefaults_Map(t *testing.T) {
	type testStruct struct {
		Tags map[string]string
	}

	s := &testStruct{}
	if err := SetDefaults(s, "default"); err != nil {
		t.Fatalf("SetDefaults failed: %v", err)
	}

	if s.Tags == nil {
		t.Fatal("Tags should not be nil")
	}
	if _, ok := s.Tags["example_key"]; !ok {
		t.Error("Tags should have example_key")
	}
}

func TestSetDefaults_NotPointer(t *testing.T) {
	type testStruct struct {
		Name string `default:"hello"`
	}

	s := testStruct{}
	err := SetDefaults(s, "default")
	if err == nil {
		t.Error("expected error for non-pointer, got nil")
	}
}

func TestSetDefaults_Pointer(t *testing.T) {
	type testStruct struct {
		Value *int `default:"99"`
	}

	s := &testStruct{}
	if err := SetDefaults(s, "default"); err != nil {
		t.Fatalf("SetDefaults failed: %v", err)
	}

	if s.Value == nil {
		t.Fatal("Value should not be nil")
	}
	if *s.Value != 99 {
		t.Errorf("Value = %d, want %d", *s.Value, 99)
	}
}

func TestSetDefaults_PointerPreserveExisting(t *testing.T) {
	type testStruct struct {
		Value *int `default:"99"`
	}

	existing := 42
	s := &testStruct{Value: &existing}
	if err := SetDefaults(s, "default"); err != nil {
		t.Fatalf("SetDefaults failed: %v", err)
	}

	if *s.Value != 42 {
		t.Errorf("Value = %d, want %d (should preserve existing)", *s.Value, 42)
	}
}

func TestLoad_CreatesFileIfMissing(t *testing.T) {
	type testConfig struct {
		Name string `yaml:"name" default:"test_service"`
		Port int    `yaml:"port" default:"3000"`
	}

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.yaml")

	cfg, err := Load[testConfig](configFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Name != "test_service" {
		t.Errorf("Name = %q, want %q", cfg.Name, "test_service")
	}
	if cfg.Port != 3000 {
		t.Errorf("Port = %d, want %d", cfg.Port, 3000)
	}

	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		t.Error("config file was not created")
	}
}

func TestLoad_ReadsExistingFile(t *testing.T) {
	type testConfig struct {
		Name string `yaml:"name" default:"test_service"`
		Port int    `yaml:"port" default:"3000"`
	}

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.yaml")

	content := []byte("name: custom_service\nport: 9999\n")
	if err := os.WriteFile(configFile, content, 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := Load[testConfig](configFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Name != "custom_service" {
		t.Errorf("Name = %q, want %q", cfg.Name, "custom_service")
	}
	if cfg.Port != 9999 {
		t.Errorf("Port = %d, want %d", cfg.Port, 9999)
	}
}

func TestLoad_MergesDefaultsWithExisting(t *testing.T) {
	type testConfig struct {
		Name    string `yaml:"name" default:"test_service"`
		Port    int    `yaml:"port" default:"3000"`
		Version string `yaml:"version" default:"1.0.0"`
	}

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.yaml")

	// File only has Name, missing Port and Version
	content := []byte("name: my_service\n")
	if err := os.WriteFile(configFile, content, 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := Load[testConfig](configFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Name != "my_service" {
		t.Errorf("Name = %q, want %q (from file)", cfg.Name, "my_service")
	}
	if cfg.Port != 3000 {
		t.Errorf("Port = %d, want %d (from default)", cfg.Port, 3000)
	}
	if cfg.Version != "1.0.0" {
		t.Errorf("Version = %q, want %q (from default)", cfg.Version, "1.0.0")
	}
}

func TestSetValueFromEnv_Fallback(t *testing.T) {
	result := SetValueFromEnv("/nonexistent/path", "NONEXISTENT_VAR_XYZ_TEST", "fallback_value")
	if result != "fallback_value" {
		t.Errorf("SetValueFromEnv = %q, want %q", result, "fallback_value")
	}
}

func TestSetValueFromEnv_EnvVar(t *testing.T) {
	t.Setenv("TEST_SECRET_VAR_NVX", "env_value")
	result := SetValueFromEnv("/nonexistent/path", "TEST_SECRET_VAR_NVX", "fallback")
	if result != "env_value" {
		t.Errorf("SetValueFromEnv = %q, want %q", result, "env_value")
	}
}

func TestSetValueFromEnv_File(t *testing.T) {
	tmpDir := t.TempDir()
	secretFile := filepath.Join(tmpDir, "secret")
	if err := os.WriteFile(secretFile, []byte("  file_secret  \n"), 0644); err != nil {
		t.Fatalf("failed to write secret file: %v", err)
	}

	result := SetValueFromEnv(secretFile, "IGNORED_VAR", "fallback")
	if result != "file_secret" {
		t.Errorf("SetValueFromEnv = %q, want %q", result, "file_secret")
	}
}

func TestLoadInto_ExistingStruct(t *testing.T) {
	type testConfig struct {
		Name string `yaml:"name" default:"default_name"`
		Port int    `yaml:"port" default:"3000"`
	}

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.yaml")

	cfg := &testConfig{Name: "pre_set_name"}
	if err := LoadInto(configFile, cfg); err != nil {
		t.Fatalf("LoadInto failed: %v", err)
	}

	// Pre-set name should be preserved (non-zero value)
	if cfg.Name != "pre_set_name" {
		t.Errorf("Name = %q, want %q", cfg.Name, "pre_set_name")
	}
	// Port should get default
	if cfg.Port != 3000 {
		t.Errorf("Port = %d, want %d", cfg.Port, 3000)
	}
}
