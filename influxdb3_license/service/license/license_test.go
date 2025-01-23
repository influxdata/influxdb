package license_test

import (
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/license"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/license/signer"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/license/verifier"
	"github.com/stretchr/testify/require"
)

func Test_LicenseCreateAndVerify(t *testing.T) {
	now := time.Now()
	timeFn := func() time.Time { return now }

	licDuration_30Days := 30 * 24 * time.Hour

	// Setup table of tests
	tests := []struct {
		name        string
		skip        bool
		email       string
		nodeID      string
		instanceID  string
		duration    time.Duration
		privKey     string
		pubKey      string
		expErr      bool
		errContains string
		expClaims   *license.Claims
		timeFn      func() time.Time
	}{
		{
			name:       "Google KMS valid license",
			skip:       true,
			email:      "jdoe@some.com",
			nodeID:     "node123",
			instanceID: "instance123",
			duration:   licDuration_30Days,
			privKey:    "projects/influxdata-team-clustered/locations/global/keyRings/clustered-licensing/cryptoKeys/signing-key/cryptoKeyVersions/1",
			pubKey:     "gcloud-kms_global_clustered-licensing_signing-key_v1.pem",
			expErr:     false,
			expClaims: &license.Claims{
				RegisteredClaims: jwt.RegisteredClaims{
					Issuer:    license.IssuerDefault,
					IssuedAt:  jwt.NewNumericDate(now),
					ExpiresAt: jwt.NewNumericDate(now.Add(licDuration_30Days)),
				},
				LicenseExp: jwt.NewNumericDate(now.Add(licDuration_30Days)),
				Email:      "jdoe@some.com",
				NodeID:     "node123",
				InstanceID: "instance123",
			},
			timeFn: timeFn,
		},
		{
			name:        "Google KMS expired license",
			skip:        true,
			email:       "jdoe@some.com",
			nodeID:      "node123",
			instanceID:  "instance123",
			duration:    0,
			privKey:     "projects/influxdata-team-clustered/locations/global/keyRings/clustered-licensing/cryptoKeys/signing-key/cryptoKeyVersions/1",
			pubKey:      "gcloud-kms_global_clustered-licensing_signing-key_v1.pem",
			expErr:      true,
			errContains: "expired",
			expClaims: &license.Claims{
				RegisteredClaims: jwt.RegisteredClaims{
					Issuer:    license.IssuerDefault,
					IssuedAt:  jwt.NewNumericDate(now),
					ExpiresAt: jwt.NewNumericDate(now),
				},
				LicenseExp: jwt.NewNumericDate(now),
				Email:      "jdoe@some.com",
				NodeID:     "node123",
				InstanceID: "instance123",
			},
			timeFn: time.Now,
		},
		{
			name:       "Locally signed valid license",
			skip:       false,
			email:      "jdoe@some.com",
			nodeID:     "node123",
			instanceID: "instance123",
			duration:   licDuration_30Days,
			privKey:    "self-managed_test_private-key.pem",
			pubKey:     "self-managed_test_public-key.pem",
			expErr:     false,
			expClaims: &license.Claims{
				RegisteredClaims: jwt.RegisteredClaims{
					Issuer:    license.IssuerDefault,
					IssuedAt:  jwt.NewNumericDate(now),
					ExpiresAt: jwt.NewNumericDate(now.Add(licDuration_30Days)),
				},
				LicenseExp: jwt.NewNumericDate(now.Add(licDuration_30Days)),
				Email:      "jdoe@some.com",
				NodeID:     "node123",
				InstanceID: "instance123",
			},
			timeFn: timeFn,
		},
		{
			name:        "Locally signed expired license",
			skip:        false,
			email:       "jdoe@some.com",
			nodeID:      "node123",
			instanceID:  "instance123",
			duration:    0,
			privKey:     "self-managed_test_private-key.pem",
			pubKey:      "self-managed_test_public-key.pem",
			expErr:      true,
			errContains: "expired",
			expClaims: &license.Claims{
				RegisteredClaims: jwt.RegisteredClaims{
					Issuer:    license.IssuerDefault,
					IssuedAt:  jwt.NewNumericDate(now),
					ExpiresAt: jwt.NewNumericDate(now),
				},
				LicenseExp: jwt.NewNumericDate(now),
				Email:      "jdoe@some.com",
				NodeID:     "node123",
				InstanceID: "instance123",
			},
			timeFn: time.Now,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip("skipping test")
			}

			var signMethod license.Signer
			var err error

			// Create the license signer
			if strings.HasPrefix(tt.pubKey, "gcloud-kms") {
				signMethod, err = signer.NewKMSSigningMethod(tt.privKey, tt.pubKey)
				require.NoError(t, err)
			} else {
				if signMethod, err = signer.NewLocalSigningMethod(tt.privKey, tt.pubKey); err != nil {
					t.Fatalf("Error creating local signer: %v", err)
				}
			}

			// Create license creator
			signer := jwt.SigningMethodES256
			creator, err := license.NewCreator(signMethod)
			if err != nil {
				t.Fatalf("Error creating license creator: %v", err)
			}

			// Override time function for testing so we know what time
			// values to expect in the claims.
			creator.TimeFn = timeFn

			// Create license
			lic, err := creator.Create(tt.email, tt.nodeID, tt.instanceID, tt.duration)
			if err != nil {
				t.Fatalf("Error creating license: %v", err)
			}

			// Verify license
			if len(lic) == 0 {
				t.Fatalf("License is empty")
			}

			verifier, err := verifier.NewVerifier(signer)
			if err != nil {
				t.Fatalf("Error creating verifier: %v", err)
			}

			token, err := verifier.Verify(lic)
			if err != nil {
				if tt.expErr && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("Expected error to contain %q, got %v", tt.errContains, err)
				} else if !tt.expErr {
					t.Fatalf("Error verifying license: %v", err)
				}
				return
			} else if err == nil && tt.expErr {
				t.Fatalf("Expected error verifying license")
			}

			// Validate claims
			claims, ok := token.Claims.(*license.Claims)
			if !ok {
				t.Fatalf("Invalid claims type")
			}

			if claims.Email != tt.expClaims.Email {
				t.Errorf("Expected email %q, got %q", tt.expClaims.Email, claims.Email)
			}

			if claims.NodeID != tt.expClaims.NodeID {
				t.Errorf("Expected node ID %q, got %q", tt.expClaims.NodeID, claims.NodeID)
			}

			if claims.InstanceID != tt.expClaims.InstanceID {
				t.Errorf("Expected instance ID %q, got %q", tt.expClaims.InstanceID, claims.InstanceID)
			}

			if claims.LicenseExp.Time != tt.expClaims.LicenseExp.Time {
				t.Errorf("Expected license expiration %v, got %v", tt.expClaims.LicenseExp.Time, claims.LicenseExp.Time)
			}
		})
	}
}
