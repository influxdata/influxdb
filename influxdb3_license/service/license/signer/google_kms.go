package signer

import (
	"context"
	"crypto"
	"encoding/asn1"
	"fmt"
	"math/big"

	kms "cloud.google.com/go/kms/apiv1"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	jwt "github.com/golang-jwt/jwt/v5"
)

// KMSSigningMethod is a custom JWT signing method that uses Google Cloud KMS.
// It implements the jwt.SigningMethod interface.
type KMSSigningMethod struct {
	jwt.SigningMethod
}

// NewKMSSigningMethod creates a new KMSSigningMethod instance.
func NewKMSSigningMethod() *KMSSigningMethod {
	return &KMSSigningMethod{
		SigningMethod: jwt.SigningMethodES256,
	}
}

// Sign implements the jwt.SigningMethod interface, which signs the given
// signing string using Google Cloud KMS.
func (m *KMSSigningMethod) Sign(signingString string, privKey interface{}) ([]byte, error) {
	// Hash the signing string
	hasher := crypto.SHA256.New()
	hasher.Write([]byte(signingString))
	digest := hasher.Sum(nil)

	// Convert privKey to KMS key name
	kmsKeyPath, ok := privKey.(string)
	if !ok {
		return nil, fmt.Errorf("invalid key type: expected string, got %T", privKey)
	}

	// Sign with KMS
	req := &kmspb.AsymmetricSignRequest{
		Name: kmsKeyPath,
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

// ecdsaSignature is used for ASN.1 DER decoding
type ecdsaSignature struct {
	R, S *big.Int
}
