package index

import (
	"fmt"
	"regexp"
)

// MatchType defines the type of label matching operation.
type MatchType int

const (
	// MatchEqual matches labels that are exactly equal to the value.
	// Example: host="server1"
	MatchEqual MatchType = iota

	// MatchNotEqual matches labels that are not equal to the value.
	// Example: host!="server1"
	MatchNotEqual

	// MatchRegexp matches labels that match the regular expression.
	// Example: host=~"server.*"
	MatchRegexp

	// MatchNotRegexp matches labels that don't match the regular expression.
	// Example: host!~"server.*"
	MatchNotRegexp
)

// String returns the string representation of the match type.
func (m MatchType) String() string {
	switch m {
	case MatchEqual:
		return "="
	case MatchNotEqual:
		return "!="
	case MatchRegexp:
		return "=~"
	case MatchNotRegexp:
		return "!~"
	default:
		return "unknown"
	}
}

// Matcher represents a label matching condition.
// It combines a label name, match type, and value to filter series.
type Matcher struct {
	Name  string    // Label name (e.g., "host", "metric")
	Type  MatchType // Match operation type
	Value string    // Value to match against

	// regex is the compiled regular expression for MatchRegexp and MatchNotRegexp.
	// It's lazily compiled on first use and cached for performance.
	regex *regexp.Regexp
}

// NewMatcher creates a new label matcher.
// For regex matchers, the regex is compiled and validated.
func NewMatcher(matchType MatchType, name, value string) (*Matcher, error) {
	if name == "" {
		return nil, fmt.Errorf("label name cannot be empty")
	}

	m := &Matcher{
		Name:  name,
		Type:  matchType,
		Value: value,
	}

	// Compile regex for regex matchers
	if matchType == MatchRegexp || matchType == MatchNotRegexp {
		if value == "" {
			return nil, fmt.Errorf("regex value cannot be empty")
		}
		re, err := regexp.Compile(value)
		if err != nil {
			return nil, fmt.Errorf("invalid regex %q: %w", value, err)
		}
		m.regex = re
	}

	return m, nil
}

// Matches checks if the given label value matches this matcher's condition.
func (m *Matcher) Matches(value string) bool {
	switch m.Type {
	case MatchEqual:
		return value == m.Value
	case MatchNotEqual:
		return value != m.Value
	case MatchRegexp:
		return m.regex != nil && m.regex.MatchString(value)
	case MatchNotRegexp:
		return m.regex != nil && !m.regex.MatchString(value)
	default:
		return false
	}
}

// String returns a string representation of the matcher.
// Example: host="server1", metric=~"cpu.*"
func (m *Matcher) String() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value)
}

// MatchesLabels checks if a set of labels matches this matcher.
// Returns true if the label exists and matches the condition.
// For NotEqual and NotRegexp, also returns true if the label doesn't exist.
func (m *Matcher) MatchesLabels(labels map[string]string) bool {
	value, exists := labels[m.Name]

	switch m.Type {
	case MatchEqual:
		return exists && value == m.Value
	case MatchNotEqual:
		// If label doesn't exist, it's considered "not equal" to any value
		return !exists || value != m.Value
	case MatchRegexp:
		return exists && m.regex != nil && m.regex.MatchString(value)
	case MatchNotRegexp:
		// If label doesn't exist, it's considered "not matching" the regex
		return !exists || (m.regex != nil && !m.regex.MatchString(value))
	default:
		return false
	}
}

// Matchers is a collection of label matchers that must all be satisfied.
type Matchers []*Matcher

// Matches checks if all matchers in the collection match the given labels.
func (ms Matchers) Matches(labels map[string]string) bool {
	for _, m := range ms {
		if !m.MatchesLabels(labels) {
			return false
		}
	}
	return true
}

// String returns a string representation of all matchers.
// Example: {host="server1", metric=~"cpu.*"}
func (ms Matchers) String() string {
	if len(ms) == 0 {
		return "{}"
	}

	s := "{"
	for i, m := range ms {
		if i > 0 {
			s += ", "
		}
		s += m.String()
	}
	s += "}"
	return s
}

// Validate checks if the matchers are valid.
// Returns an error if any matcher is invalid.
func (ms Matchers) Validate() error {
	if len(ms) == 0 {
		return fmt.Errorf("at least one matcher required")
	}

	// Check for duplicate label names
	seen := make(map[string]bool)
	for _, m := range ms {
		if seen[m.Name] {
			return fmt.Errorf("duplicate matcher for label %q", m.Name)
		}
		seen[m.Name] = true
	}

	return nil
}

// Equal checks if two matchers are equal.
func (m *Matcher) Equal(other *Matcher) bool {
	if m == nil || other == nil {
		return m == other
	}
	return m.Name == other.Name && m.Type == other.Type && m.Value == other.Value
}

// MustNewMatcher creates a new matcher and panics if there's an error.
// This is useful for test cases and initialization code where the matcher
// is known to be valid.
func MustNewMatcher(matchType MatchType, name, value string) *Matcher {
	m, err := NewMatcher(matchType, name, value)
	if err != nil {
		panic(fmt.Sprintf("failed to create matcher: %v", err))
	}
	return m
}
