package signer

import (
	"context"
	"crypto"
	"encoding/asn1"
	"fmt"
	"math/big"
	"strings"

	kms "cloud.google.com/go/kms/apiv1"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/keyring"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/license"
)

// KMSSigningMethod is a custom JWT signing method that uses Google Cloud KMS.
// It implements the jwt.SigningMethod interface.
type KMSSigningMethod struct {
	jwt.SigningMethod
	privKey string
	pubKey  string
}

// NewKMSSigningMethod creates a new KMSSigningMethod instance.
func NewKMSSigningMethod(privKey, pubKey string) (license.Signer, error) {
	// Make sure the public key is available in the local keyring. Otherwise,
	// the license cannot be verified.
	kr, err := keyring.LoadKeys()
	if err != nil {
		return nil, fmt.Errorf("license creator failed to load local keyring: %w", err)
	}

	if _, err := kr.GetKey(pubKey); err != nil {
		return nil, fmt.Errorf("could not find pubKey in local keyring: %w", err)
	}

	// If the pub key is for Google Cloud KMS, make sure it is in the correct format
	// to convert to a KMS key path.
	if strings.HasPrefix(pubKey, "gcloud-kms") {
		_, err := kr.KMSPrivateKey("any", pubKey)
		if err != nil {
			return nil, fmt.Errorf("pubKey appears to be a Google KMS key but couldn't derive path to KMS privKey resrource: %w", err)
		}
	}
	return &KMSSigningMethod{
		SigningMethod: jwt.SigningMethodES256,
		privKey:       privKey,
		pubKey:        pubKey,
	}, nil
}

// Sign implements the jwt.SigningMethod interface, which signs the given
// signing string using Google Cloud KMS.
func (m *KMSSigningMethod) Sign(signingString string, _privKey interface{}) ([]byte, error) {
	// Hash the signing string
	hasher := crypto.SHA256.New()
	hasher.Write([]byte(signingString))
	digest := hasher.Sum(nil)

	// Sign with KMS
	req := &kmspb.AsymmetricSignRequest{
		Name: m.privKey,
		Digest: &kmspb.Digest{
			Digest: &kmspb.Digest_Sha256{
				Sha256: digest,
			},
		},
	}

	ctx := context.Background()
	client, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := client.AsymmetricSign(context.Background(), req)
	if err != nil {
		return nil, err
	}

	// resp.Signature is DER-encoded ECDSA signature
	// Decode DER signature into R and S
	var sig ecdsaSignature
	if _, err := asn1.Unmarshal(resp.Signature, &sig); err != nil {
		return nil, fmt.Errorf("failed to decode DER signature: %w", err)
	}

	// Convert DER-encoded R/S to raw format (IEEE P1363)
	curveBits := 256
	byteLen := (curveBits + 7) / 8
	rBytes := sig.R.FillBytes(make([]byte, byteLen))
	sBytes := sig.S.FillBytes(make([]byte, byteLen))
	rawSig := append(rBytes, sBytes...)

	// Return rawSig as the signature bytes (the jwt library will handle base64)
	return rawSig, nil
}

func (m *KMSSigningMethod) Kid() string {
	return m.pubKey
}

// ecdsaSignature is used for ASN.1 DER decoding
type ecdsaSignature struct {
	R, S *big.Int
}
