package tlsconfig

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_NewConfig(t *testing.T) {
	c := NewConfig()
	require.Empty(t, c.Ciphers)
	require.Empty(t, c.MinVersion)
	require.Empty(t, c.MaxVersion)

	// An unset Config is valid and parses to a nil *tls.Config so callers can
	// distinguish "nothing configured" from "configured".
	require.NoError(t, c.Validate())
	parsed, err := c.Parse()
	require.NoError(t, err)
	require.Nil(t, parsed)
}

func TestConfig_Parse(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected *tls.Config
	}{
		{
			name:     "empty config parses to nil",
			config:   Config{},
			expected: nil,
		},
		{
			name:   "single cipher",
			config: Config{Ciphers: []string{"TLS_AES_128_GCM_SHA256"}},
			expected: &tls.Config{
				CipherSuites: []uint16{tls.TLS_AES_128_GCM_SHA256},
			},
		},
		{
			name: "multiple ciphers preserve order",
			config: Config{Ciphers: []string{
				"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
				"TLS_AES_256_GCM_SHA384",
			}},
			expected: &tls.Config{
				CipherSuites: []uint16{
					tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_AES_256_GCM_SHA384,
				},
			},
		},
		{
			name:   "cipher names are case insensitive",
			config: Config{Ciphers: []string{"tls_aes_128_gcm_sha256"}},
			expected: &tls.Config{
				CipherSuites: []uint16{tls.TLS_AES_128_GCM_SHA256},
			},
		},
		{
			name:     "min version only",
			config:   Config{MinVersion: "TLS1.2"},
			expected: &tls.Config{MinVersion: tls.VersionTLS12},
		},
		{
			name:     "max version only",
			config:   Config{MaxVersion: "TLS1.3"},
			expected: &tls.Config{MaxVersion: tls.VersionTLS13},
		},
		{
			name:     "version names are case insensitive",
			config:   Config{MinVersion: "tls1.3"},
			expected: &tls.Config{MinVersion: tls.VersionTLS13},
		},
		{
			name:     "numeric version aliases",
			config:   Config{MinVersion: "1.0", MaxVersion: "1.3"},
			expected: &tls.Config{MinVersion: tls.VersionTLS10, MaxVersion: tls.VersionTLS13},
		},
		{
			name: "ciphers and versions together",
			config: Config{
				Ciphers:    []string{"TLS_AES_128_GCM_SHA256"},
				MinVersion: "TLS1.2",
				MaxVersion: "TLS1.3",
			},
			expected: &tls.Config{
				CipherSuites: []uint16{tls.TLS_AES_128_GCM_SHA256},
				MinVersion:   tls.VersionTLS12,
				MaxVersion:   tls.VersionTLS13,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := tt.config.Parse()
			require.NoError(t, err)
			require.Equal(t, tt.expected, parsed)

			// Anything that parses cleanly must also validate cleanly.
			require.NoError(t, tt.config.Validate())
		})
	}
}

func TestConfig_ParseErrors(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		errMsg string
	}{
		{
			name:   "unknown cipher",
			config: Config{Ciphers: []string{"TLS_NOT_A_REAL_CIPHER"}},
			errMsg: `unknown cipher suite: "TLS_NOT_A_REAL_CIPHER". available ciphers: `,
		},
		{
			name:   "unknown cipher among valid ciphers",
			config: Config{Ciphers: []string{"TLS_AES_128_GCM_SHA256", "BOGUS"}},
			errMsg: `unknown cipher suite: "BOGUS". available ciphers: `,
		},
		{
			name:   "unknown min version",
			config: Config{MinVersion: "TLS9.9"},
			errMsg: `unknown tls version: "TLS9.9". available versions: `,
		},
		{
			name:   "unknown max version",
			config: Config{MaxVersion: "nope"},
			errMsg: `unknown tls version: "nope". available versions: `,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := tt.config.Parse()
			require.ErrorContains(t, err, tt.errMsg)
			require.Nil(t, parsed)

			// Validate is a thin wrapper around Parse and must report the same error.
			require.ErrorContains(t, tt.config.Validate(), tt.errMsg)
		})
	}
}

func TestConfig_UnknownCipherListsAvailable(t *testing.T) {
	err := unknownCipher("bogus")
	require.ErrorContains(t, err, `unknown cipher suite: "bogus"`)

	// The advice is only useful if it names ciphers the user can actually pick.
	require.ErrorContains(t, err, "TLS_AES_128_GCM_SHA256")
	require.ErrorContains(t, err, "TLS_RSA_WITH_AES_128_CBC_SHA")
}

func TestConfig_UnknownVersionListsAvailable(t *testing.T) {
	err := unknownVersion("bogus")
	require.ErrorContains(t, err, `unknown tls version: "bogus"`)
	require.ErrorContains(t, err, "TLS1.0")
	require.ErrorContains(t, err, "TLS1.3")

	// The numeric aliases ("1.0", "1.3") are deliberately omitted from the
	// suggestions to avoid a confusing, duplicated list.
	require.NotContains(t, err.Error(), " 1.0")
	require.NotContains(t, err.Error(), " 1.3")
}
