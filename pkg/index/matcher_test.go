package index

import (
	"testing"
)

func TestMatchType_String(t *testing.T) {
	tests := []struct {
		matchType MatchType
		want      string
	}{
		{MatchEqual, "="},
		{MatchNotEqual, "!="},
		{MatchRegexp, "=~"},
		{MatchNotRegexp, "!~"},
		{MatchType(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.matchType.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewMatcher(t *testing.T) {
	tests := []struct {
		name      string
		matchType MatchType
		labelName string
		value     string
		wantErr   bool
	}{
		{
			name:      "valid equal matcher",
			matchType: MatchEqual,
			labelName: "host",
			value:     "server1",
			wantErr:   false,
		},
		{
			name:      "valid not equal matcher",
			matchType: MatchNotEqual,
			labelName: "env",
			value:     "prod",
			wantErr:   false,
		},
		{
			name:      "valid regex matcher",
			matchType: MatchRegexp,
			labelName: "host",
			value:     "server.*",
			wantErr:   false,
		},
		{
			name:      "valid not regex matcher",
			matchType: MatchNotRegexp,
			labelName: "host",
			value:     "test.*",
			wantErr:   false,
		},
		{
			name:      "empty label name",
			matchType: MatchEqual,
			labelName: "",
			value:     "value",
			wantErr:   true,
		},
		{
			name:      "invalid regex",
			matchType: MatchRegexp,
			labelName: "host",
			value:     "[invalid",
			wantErr:   true,
		},
		{
			name:      "empty regex value",
			matchType: MatchRegexp,
			labelName: "host",
			value:     "",
			wantErr:   true,
		},
		{
			name:      "empty value for equal matcher is valid",
			matchType: MatchEqual,
			labelName: "host",
			value:     "",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMatcher(tt.matchType, tt.labelName, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewMatcher() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if got.Name != tt.labelName {
					t.Errorf("Name = %v, want %v", got.Name, tt.labelName)
				}
				if got.Type != tt.matchType {
					t.Errorf("Type = %v, want %v", got.Type, tt.matchType)
				}
				if got.Value != tt.value {
					t.Errorf("Value = %v, want %v", got.Value, tt.value)
				}
			}
		})
	}
}

func TestMatcher_Matches(t *testing.T) {
	tests := []struct {
		name      string
		matcher   *Matcher
		value     string
		wantMatch bool
	}{
		{
			name:      "equal - match",
			matcher:   MustNewMatcher(MatchEqual, "host", "server1"),
			value:     "server1",
			wantMatch: true,
		},
		{
			name:      "equal - no match",
			matcher:   MustNewMatcher(MatchEqual, "host", "server1"),
			value:     "server2",
			wantMatch: false,
		},
		{
			name:      "not equal - match",
			matcher:   MustNewMatcher(MatchNotEqual, "host", "server1"),
			value:     "server2",
			wantMatch: true,
		},
		{
			name:      "not equal - no match",
			matcher:   MustNewMatcher(MatchNotEqual, "host", "server1"),
			value:     "server1",
			wantMatch: false,
		},
		{
			name:      "regex - match",
			matcher:   MustNewMatcher(MatchRegexp, "host", "server[0-9]+"),
			value:     "server123",
			wantMatch: true,
		},
		{
			name:      "regex - no match",
			matcher:   MustNewMatcher(MatchRegexp, "host", "server[0-9]+"),
			value:     "database",
			wantMatch: false,
		},
		{
			name:      "regex - partial match",
			matcher:   MustNewMatcher(MatchRegexp, "host", "server"),
			value:     "myserver",
			wantMatch: true,
		},
		{
			name:      "regex - anchored match",
			matcher:   MustNewMatcher(MatchRegexp, "host", "^server$"),
			value:     "server",
			wantMatch: true,
		},
		{
			name:      "regex - anchored no match",
			matcher:   MustNewMatcher(MatchRegexp, "host", "^server$"),
			value:     "myserver",
			wantMatch: false,
		},
		{
			name:      "not regex - match",
			matcher:   MustNewMatcher(MatchNotRegexp, "host", "server[0-9]+"),
			value:     "database",
			wantMatch: true,
		},
		{
			name:      "not regex - no match",
			matcher:   MustNewMatcher(MatchNotRegexp, "host", "server[0-9]+"),
			value:     "server123",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.matcher.Matches(tt.value); got != tt.wantMatch {
				t.Errorf("Matches(%q) = %v, want %v", tt.value, got, tt.wantMatch)
			}
		})
	}
}

func TestMatcher_MatchesLabels(t *testing.T) {
	tests := []struct {
		name      string
		matcher   *Matcher
		labels    map[string]string
		wantMatch bool
	}{
		{
			name:    "equal - match",
			matcher: MustNewMatcher(MatchEqual, "host", "server1"),
			labels: map[string]string{
				"host":   "server1",
				"metric": "cpu",
			},
			wantMatch: true,
		},
		{
			name:    "equal - no match",
			matcher: MustNewMatcher(MatchEqual, "host", "server1"),
			labels: map[string]string{
				"host":   "server2",
				"metric": "cpu",
			},
			wantMatch: false,
		},
		{
			name:    "equal - label missing",
			matcher: MustNewMatcher(MatchEqual, "host", "server1"),
			labels: map[string]string{
				"metric": "cpu",
			},
			wantMatch: false,
		},
		{
			name:    "not equal - match (different value)",
			matcher: MustNewMatcher(MatchNotEqual, "host", "server1"),
			labels: map[string]string{
				"host":   "server2",
				"metric": "cpu",
			},
			wantMatch: true,
		},
		{
			name:    "not equal - match (label missing)",
			matcher: MustNewMatcher(MatchNotEqual, "host", "server1"),
			labels: map[string]string{
				"metric": "cpu",
			},
			wantMatch: true,
		},
		{
			name:    "not equal - no match",
			matcher: MustNewMatcher(MatchNotEqual, "host", "server1"),
			labels: map[string]string{
				"host": "server1",
			},
			wantMatch: false,
		},
		{
			name:    "regex - match",
			matcher: MustNewMatcher(MatchRegexp, "host", "server[0-9]+"),
			labels: map[string]string{
				"host": "server123",
			},
			wantMatch: true,
		},
		{
			name:    "regex - no match",
			matcher: MustNewMatcher(MatchRegexp, "host", "server[0-9]+"),
			labels: map[string]string{
				"host": "database",
			},
			wantMatch: false,
		},
		{
			name:    "regex - label missing",
			matcher: MustNewMatcher(MatchRegexp, "host", "server.*"),
			labels: map[string]string{
				"metric": "cpu",
			},
			wantMatch: false,
		},
		{
			name:    "not regex - match (different value)",
			matcher: MustNewMatcher(MatchNotRegexp, "host", "server[0-9]+"),
			labels: map[string]string{
				"host": "database",
			},
			wantMatch: true,
		},
		{
			name:    "not regex - match (label missing)",
			matcher: MustNewMatcher(MatchNotRegexp, "host", "server.*"),
			labels: map[string]string{
				"metric": "cpu",
			},
			wantMatch: true,
		},
		{
			name:    "not regex - no match",
			matcher: MustNewMatcher(MatchNotRegexp, "host", "server[0-9]+"),
			labels: map[string]string{
				"host": "server123",
			},
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.matcher.MatchesLabels(tt.labels); got != tt.wantMatch {
				t.Errorf("MatchesLabels(%v) = %v, want %v", tt.labels, got, tt.wantMatch)
			}
		})
	}
}

func TestMatcher_String(t *testing.T) {
	tests := []struct {
		name    string
		matcher *Matcher
		want    string
	}{
		{
			name:    "equal",
			matcher: MustNewMatcher(MatchEqual, "host", "server1"),
			want:    `host="server1"`,
		},
		{
			name:    "not equal",
			matcher: MustNewMatcher(MatchNotEqual, "env", "prod"),
			want:    `env!="prod"`,
		},
		{
			name:    "regex",
			matcher: MustNewMatcher(MatchRegexp, "host", "server.*"),
			want:    `host=~"server.*"`,
		},
		{
			name:    "not regex",
			matcher: MustNewMatcher(MatchNotRegexp, "host", "test.*"),
			want:    `host!~"test.*"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.matcher.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatcher_Equal(t *testing.T) {
	m1 := MustNewMatcher(MatchEqual, "host", "server1")
	m2 := MustNewMatcher(MatchEqual, "host", "server1")
	m3 := MustNewMatcher(MatchEqual, "host", "server2")
	m4 := MustNewMatcher(MatchNotEqual, "host", "server1")

	if !m1.Equal(m2) {
		t.Error("Equal matchers not equal")
	}
	if m1.Equal(m3) {
		t.Error("Different value matchers equal")
	}
	if m1.Equal(m4) {
		t.Error("Different type matchers equal")
	}
	if m1.Equal(nil) {
		t.Error("Matcher equal to nil")
	}

	var nilMatcher *Matcher
	if !nilMatcher.Equal(nil) {
		t.Error("Nil matcher not equal to nil")
	}
}

func TestMatchers_Matches(t *testing.T) {
	tests := []struct {
		name      string
		matchers  Matchers
		labels    map[string]string
		wantMatch bool
	}{
		{
			name: "all match",
			matchers: Matchers{
				MustNewMatcher(MatchEqual, "host", "server1"),
				MustNewMatcher(MatchEqual, "metric", "cpu"),
			},
			labels: map[string]string{
				"host":   "server1",
				"metric": "cpu",
			},
			wantMatch: true,
		},
		{
			name: "one doesn't match",
			matchers: Matchers{
				MustNewMatcher(MatchEqual, "host", "server1"),
				MustNewMatcher(MatchEqual, "metric", "memory"),
			},
			labels: map[string]string{
				"host":   "server1",
				"metric": "cpu",
			},
			wantMatch: false,
		},
		{
			name: "mixed matchers - all match",
			matchers: Matchers{
				MustNewMatcher(MatchEqual, "env", "prod"),
				MustNewMatcher(MatchRegexp, "host", "server[0-9]+"),
				MustNewMatcher(MatchNotEqual, "dc", "us-west"),
			},
			labels: map[string]string{
				"env":  "prod",
				"host": "server123",
				"dc":   "us-east",
			},
			wantMatch: true,
		},
		{
			name: "empty matchers",
			matchers: Matchers{},
			labels: map[string]string{
				"host": "server1",
			},
			wantMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.matchers.Matches(tt.labels); got != tt.wantMatch {
				t.Errorf("Matches(%v) = %v, want %v", tt.labels, got, tt.wantMatch)
			}
		})
	}
}

func TestMatchers_String(t *testing.T) {
	tests := []struct {
		name     string
		matchers Matchers
		want     string
	}{
		{
			name:     "empty",
			matchers: Matchers{},
			want:     "{}",
		},
		{
			name: "single matcher",
			matchers: Matchers{
				MustNewMatcher(MatchEqual, "host", "server1"),
			},
			want: `{host="server1"}`,
		},
		{
			name: "multiple matchers",
			matchers: Matchers{
				MustNewMatcher(MatchEqual, "host", "server1"),
				MustNewMatcher(MatchRegexp, "metric", "cpu.*"),
			},
			want: `{host="server1", metric=~"cpu.*"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.matchers.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchers_Validate(t *testing.T) {
	tests := []struct {
		name     string
		matchers Matchers
		wantErr  bool
	}{
		{
			name: "valid matchers",
			matchers: Matchers{
				MustNewMatcher(MatchEqual, "host", "server1"),
				MustNewMatcher(MatchEqual, "metric", "cpu"),
			},
			wantErr: false,
		},
		{
			name:     "empty matchers",
			matchers: Matchers{},
			wantErr:  true,
		},
		{
			name: "duplicate label names",
			matchers: Matchers{
				MustNewMatcher(MatchEqual, "host", "server1"),
				MustNewMatcher(MatchNotEqual, "host", "server2"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.matchers.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMustNewMatcher(t *testing.T) {
	// Valid matcher should not panic
	m := MustNewMatcher(MatchEqual, "host", "server1")
	if m == nil {
		t.Error("MustNewMatcher returned nil")
	}

	// Invalid matcher should panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustNewMatcher with invalid regex did not panic")
		}
	}()
	MustNewMatcher(MatchRegexp, "host", "[invalid")
}
