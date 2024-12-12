package keyring

import (
	"embed"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

//go:embed keys
var keyFS embed.FS

type Keyring struct {
	Keys map[string][]byte
}

// LoadKeys loads the public keys from the embedded filesystem.
func LoadKeys() (*Keyring, error) {
	keys, err := keyFS.ReadDir("keys")
	if err != nil {
		return nil, err
	}

	keyring := &Keyring{
		Keys: make(map[string][]byte, len(keys)),
	}

	for _, key := range keys {
		if key.IsDir() {
			continue
		}

		keyBytes, err := keyFS.ReadFile("keys/" + key.Name())
		if err != nil {
			return nil, err
		}

		keyring.Keys[key.Name()] = keyBytes
	}

	return keyring, nil
}

// GetKey returns the key for the given key ID.
func (k *Keyring) GetKey(kid string) ([]byte, error) {
	key, ok := k.Keys[kid]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return key, nil
}

// KMSPrivateKey returns the path to the KMS private key for the given file
// name of a public key stored in the keyring.
func (k *Keyring) KMSPrivateKey(projectID, kid string) (string, error) {
	/* Example:

	   Input:
	   "gcloud-kms_global_clustered-licensing_signing-key_v1.pem"

	   Desired Output:
	   "projects/influxdata-team-clustered/locations/global/keyRings/clustered-licensing/cryptoKeys/signing-key/cryptoKeyVersions/1"

	*/

	// Check that the key exists
	_, ok := k.Keys[kid]
	if !ok {
		return "", ErrKeyNotFound
	}

	// Check that the key ID starts with "gcloud-kms"
	if !strings.HasPrefix(kid, "gcloud-kms") {
		return "", fmt.Errorf("not a glcoud-kms key ID: %s", kid)
	}

	// Strip the .pem extension
	name := strings.TrimSuffix(kid, ".pem")

	// name should now be: "gcloud-kms_global_clustered-licensing_signing-key_v1"
	parts := strings.Split(name, "_")
	if len(parts) < 5 {
		return "", fmt.Errorf("invalid key ID format: %s", kid)
	}

	// parts[0] = "gcloud-kms"
	// parts[1] = location (e.g. "global")
	// parts[2] = keyRing (e.g. "clustered-licensing")
	// parts[3] = cryptoKey (e.g. "signing-key")
	// parts[4] = version prefixed with 'v', e.g. "v1"
	location := parts[1]
	keyRing := parts[2]
	cryptoKey := parts[3]

	versionStr := parts[4]
	if !strings.HasPrefix(versionStr, "v") {
		return "", fmt.Errorf("version string missing 'v' prefix: %s", versionStr)
	}
	version := strings.TrimPrefix(versionStr, "v")

	// Construct the KMS resource name
	resource := fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s/cryptoKeyVersions/%s",
		projectID, location, keyRing, cryptoKey, version)

	return resource, nil
}

// KMSPublicKey returns the Google project name and key name for the given KMS resource name.
func (k *Keyring) KMSPublicKey(resource string) (project, keyname string, err error) {
	/* Example:

	   Input:
	   "projects/influxdata-team-clustered/locations/global/keyRings/clustered-licensing/cryptoKeys/signing-key/cryptoKeyVersions/1"

	   Desired Output:
	   "gcloud-kms_global_clustered-licensing_signing-key_v1.pem"
	*/

	parts := strings.Split(resource, "/")
	if len(parts) < 10 {
		return "", "", fmt.Errorf("resource string not in expected format")
	}

	project = parts[1]
	location := parts[3]
	keyRing := parts[5]
	cryptoKey := parts[7]
	version := parts[9]

	result := fmt.Sprintf("gcloud-kms_%s_%s_%s_v%s.pem", location, keyRing, cryptoKey, version)
	return project, result, nil
}
