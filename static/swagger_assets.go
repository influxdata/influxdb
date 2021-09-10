//go:build assets
// +build assets

package static

// asset returns its input arguments.
//
// There is a separate definition of asset when not using the assets build tag.
func (s *swaggerLoader) asset(swaggerData []byte, err error) ([]byte, error) {
	return swaggerData, err
}
