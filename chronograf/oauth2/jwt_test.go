package oauth2_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/chronograf/oauth2"
)

func TestAuthenticate(t *testing.T) {
	history := time.Unix(-446774400, 0)
	var tests = []struct {
		Desc   string
		Secret string
		// JWT tokens were generated at https://jwt.io/ using their Debugger
		Token     oauth2.Token
		Duration  time.Duration
		Principal oauth2.Principal
		Err       error
	}{
		{
			Desc:   "Test bad jwt token",
			Secret: "secret",
			Token:  "badtoken",
			Principal: oauth2.Principal{
				Subject: "",
			},
			Err: errors.New("token contains an invalid number of segments"),
		},
		{
			Desc:     "Test valid jwt token",
			Secret:   "secret",
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIiwibmFtZSI6IkRvYyBCcm93biIsImlhdCI6LTQ0Njc3NDQwMCwiZXhwIjotNDQ2Nzc0Mzk5LCJuYmYiOi00NDY3NzQ0MDB9.Ga0zGXWTT2CBVnnIhIO5tUAuBEVk4bKPaT4t4MU1ngo",
			Duration: time.Second,
			Principal: oauth2.Principal{
				Subject:   "/chronograf/v1/users/1",
				ExpiresAt: history.Add(time.Second),
				IssuedAt:  history,
			},
		},
		{
			Desc:     "Test valid jwt token with organization",
			Secret:   "secret",
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIiwibmFtZSI6IkRvYyBCcm93biIsIm9yZyI6IjEzMzciLCJpYXQiOi00NDY3NzQ0MDAsImV4cCI6LTQ0Njc3NDM5OSwibmJmIjotNDQ2Nzc0NDAwfQ.b38MK5liimWsvvJr4a3GNYRDJOAN7WCrfZ0FfZftqjc",
			Duration: time.Second,
			Principal: oauth2.Principal{
				Subject:      "/chronograf/v1/users/1",
				Organization: "1337",
				ExpiresAt:    history.Add(time.Second),
				IssuedAt:     history,
			},
		},
		{
			Desc:     "Test expired jwt token",
			Secret:   "secret",
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIiwibmFtZSI6IkRvYyBCcm93biIsImlhdCI6LTQ0Njc3NDQwMCwiZXhwIjotNDQ2Nzc0NDAxLCJuYmYiOi00NDY3NzQ0MDB9.vWXdm0-XQ_pW62yBpSISFFJN_yz0vqT9_INcUKTp5Q8",
			Duration: time.Second,
			Principal: oauth2.Principal{
				Subject:   "",
				ExpiresAt: history.Add(time.Second),
				IssuedAt:  history,
			},
			Err: errors.New("token is expired by 1s"),
		},
		{
			Desc:     "Test jwt token not before time",
			Secret:   "secret",
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIiwibmFtZSI6IkRvYyBCcm93biIsImlhdCI6LTQ0Njc3NDQwMCwiZXhwIjotNDQ2Nzc0NDAwLCJuYmYiOi00NDY3NzQzOTl9.TMGAhv57u1aosjc4ywKC7cElP1tKyQH7GmRF2ToAxlE",
			Duration: time.Second,
			Principal: oauth2.Principal{
				Subject:   "",
				ExpiresAt: history.Add(time.Second),
				IssuedAt:  history,
			},
			Err: errors.New("token is not valid yet"),
		},
		{
			Desc:     "Test jwt with empty subject is invalid",
			Secret:   "secret",
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOi00NDY3NzQ0MDAsImV4cCI6LTQ0Njc3NDQwMCwibmJmIjotNDQ2Nzc0NDAwfQ.gxsA6_Ei3s0f2I1TAtrrb8FmGiO25OqVlktlF_ylhX4",
			Duration: time.Second,
			Principal: oauth2.Principal{
				Subject:   "",
				ExpiresAt: history.Add(time.Second),
				IssuedAt:  history,
			},
			Err: errors.New("claim has no subject"),
		},
		{
			Desc:     "Test jwt duration matches auth duration",
			Secret:   "secret",
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOi00NDY3NzQzMDAsImlhdCI6LTQ0Njc3NDQwMCwiaXNzIjoiaGlsbHZhbGxleSIsIm5iZiI6LTQ0Njc3NDQwMCwic3ViIjoibWFydHlAcGluaGVhZC5uZXQifQ.njEjstpuIDnghSR7VyPPB9QlvJ6Q5JpR3ZEZ_8vGYfA",
			Duration: time.Second,
			Principal: oauth2.Principal{
				Subject:   "marty@pinhead.net",
				ExpiresAt: history,
				IssuedAt:  history.Add(100 * time.Second),
			},
			Err: errors.New("claims duration is different from auth lifespan"),
		},
	}
	for _, test := range tests {
		j := oauth2.JWT{
			Secret: test.Secret,
			Now: func() time.Time {
				return time.Unix(-446774400, 0)
			},
		}
		principal, err := j.ValidPrincipal(context.Background(), test.Token, test.Duration)
		if test.Err != nil && err == nil {
			t.Fatalf("Expected err %s", test.Err.Error())
		}
		if err != nil {
			if test.Err == nil {
				t.Errorf("Error in test %s authenticating with bad token: %v", test.Desc, err)
			} else if err.Error() != test.Err.Error() {
				t.Errorf("Error in test %s expected error: %v actual: %v", test.Desc, test.Err, err)
			}
		} else if test.Principal != principal {
			t.Errorf("Error in test %s; principals different; expected: %v  actual: %v", test.Desc, test.Principal, principal)
		}
	}

}

func TestToken(t *testing.T) {
	expected := oauth2.Token("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOi00NDY3NzQzOTksImlhdCI6LTQ0Njc3NDQwMCwibmJmIjotNDQ2Nzc0NDAwLCJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIn0.ofQM6yTmrmve5JeEE0RcK4_euLXuZ_rdh6bLAbtbC9M")
	history := time.Unix(-446774400, 0)
	j := oauth2.JWT{
		Secret: "secret",
		Now: func() time.Time {
			return history
		},
	}
	p := oauth2.Principal{
		Subject:   "/chronograf/v1/users/1",
		ExpiresAt: history.Add(time.Second),
		IssuedAt:  history,
	}
	if token, err := j.Create(context.Background(), p); err != nil {
		t.Errorf("Error creating token for principal: %v", err)
	} else if token != expected {
		t.Errorf("Error creating token; expected: %s  actual: %s", expected, token)
	}
}

func TestSigningMethod(t *testing.T) {
	token := oauth2.Token("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.EkN-DOsnsuRjRO6BxXemmJDm3HbxrbRzXglbN2S4sOkopdU4IsDxTI8jO19W_A4K8ZPJijNLis4EZsHeY559a4DFOd50_OqgHGuERTqYZyuhtF39yxJPAjUESwxk2J5k_4zM3O-vtd1Ghyo4IbqKKSy6J9mTniYJPenn5-HIirE")
	j := oauth2.JWT{}
	if _, err := j.ValidPrincipal(context.Background(), token, 0); err == nil {
		t.Error("Error was expected while validating incorrectly signed token")
	} else if err.Error() != "JWKSURL not specified, cannot validate RS256 signature" {
		t.Errorf("Error wanted 'JWKSURL not specified, cannot validate RS256 signature', got %s", err.Error())
	}
}

func TestGetClaims(t *testing.T) {
	var tests = []struct {
		Name         string
		TokenString  string
		JwksDocument string
		Iat          int64
		Err          error
	}{
		{
			Name:         "Valid Token with RS256 signature verified against correct JWKS document",
			TokenString:  "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IllEQlVocWRXa3NLWGRHdVgwc3l0amFVdXhoQSIsImtpZCI6IllEQlVocWRXa3NLWGRHdVgwc3l0amFVdXhoQSJ9.eyJhdWQiOiJjaHJvbm9ncmFmIiwiaXNzIjoiaHR0cHM6Ly9kc3RjaW1hYWQxcC5kc3QtaXRzLmRlL2FkZnMiLCJpYXQiOjE1MTMxNjU4ODksImV4cCI6MTUxMzE2OTQ4OSwiYXV0aF90aW1lIjoxNTEzMTY1ODg4LCJzdWIiOiJlWVYzamRsZE55RlkxcUZGSDRvQWRCdkRGZmJWZm51RzI5SGlIa1N1andrPSIsInVwbiI6ImJzY0Bkc3QtaXRzLmRlIiwidW5pcXVlX25hbWUiOiJEU1RcXGJzYyIsInNpZCI6IlMtMS01LTIxLTI1MDUxNTEzOTgtMjY2MTAyODEwOS0zNzU0MjY1ODIwLTExMDQifQ.nK51Ui4XN45SVul9igNaKFQd-F63BNstBzW-T5LBVm_ANHCEHyP3_88C3ffkkQIi3PxYacRJGtfswP35ws7YJUcNp-GoGZARqz62NpMtbQyhos6mCaVXwPoxPbrZx4AkMQgxkZwJcOzceX7mpjcT3kCth30chN3lkhzSjGrXe4ZDOAV25liS-dsdBiqDiaTB91sS534GM76qJQxFUs51oSbYTRdCN1VJ0XopMcasfVDzFrtSbyvEIVXlpKK2HplnhheqF4QHrM_3cjV_NGRr3tYLe-AGTdDXKWlJD1GDz1ECXeMGQHPoz3U8cqNsFLYBstIlCgfnBWgWsPZSvJPJUg",
			JwksDocument: `{"keys":[{"kty":"RSA","use":"sig","alg":"RS256","kid":"YDBUhqdWksKXdGuX0sytjaUuxhA","x5t":"YDBUhqdWksKXdGuX0sytjaUuxhA","n":"uwVVrs5OJRKeLUk0H5N_b4Jbvff3rxlg3WIeOO-zSSPTC5oFOc5_te0rLgVoNJJB4rNM4A7BEXI885xLrjfL3l3LHqaJetvR0tdLAnkvbUKUiGxnuGnmOsgh491P95pHPIAniy2p64FQoBbTJ0a6cF5LRuPPHKVXgjXjTydvmKrt_IVaWUDgICRsw5Bbv290SahmxcdO3akSgfsZtRkR8SmaMzAPYINi2_8P2evaKAnMQLTgUVkctaEamO_6HJ5f5sWheV7trLekU35xPVkPwShDelefnhyJcO5yICXqXzuewBEni9LrxAEJYN2rYfiFQWJy-pDe5DPUBs-IFTpctQ","e":"AQAB","x5c":["MIIC6DCCAdCgAwIBAgIQPszqLhbrpZlE+jEJTyJg7jANBgkqhkiG9w0BAQsFADAwMS4wLAYDVQQDEyVBREZTIFNpZ25pbmcgLSBkc3RjaW1hYWQxcC5kc3QtaXRzLmRlMB4XDTE3MTIwNDE0MDEwOFoXDTE4MTIwNDE0MDEwOFowMDEuMCwGA1UEAxMlQURGUyBTaWduaW5nIC0gZHN0Y2ltYWFkMXAuZHN0LWl0cy5kZTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALsFVa7OTiUSni1JNB+Tf2+CW733968ZYN1iHjjvs0kj0wuaBTnOf7XtKy4FaDSSQeKzTOAOwRFyPPOcS643y95dyx6miXrb0dLXSwJ5L21ClIhsZ7hp5jrIIePdT\/eaRzyAJ4stqeuBUKAW0ydGunBeS0bjzxylV4I1408nb5iq7fyFWllA4CAkbMOQW79vdEmoZsXHTt2pEoH7GbUZEfEpmjMwD2CDYtv\/D9nr2igJzEC04FFZHLWhGpjv+hyeX+bFoXle7ay3pFN+cT1ZD8EoQ3pXn54ciXDuciAl6l87nsARJ4vS68QBCWDdq2H4hUFicvqQ3uQz1AbPiBU6XLUCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAPHCisZyPf\/fuuQEW5LyzZSYMwBRYVR6kk\/M2ZNx6TrUEwmOb10RQ3G97bLAshN44g5lWdPYz4EOt6d2o71etIjf79f+IR0MAjEgBB2HThaHcMU9KG229Ftcauie9XeurngMawTRu60YqH7+go8EMf6a1Kdnx37DMy\/1LRlsYJVfEoOCab3GgcIdXrRSYWqsY4SVJZiTPYdqz9vmNPSXXiDSOTl6qXHV\/f53WTS2V5aIQbuJJziXlceusuVNny0o5h+j6ovZ1HhEGAu3lpD+8kY8KUqA4kXMH3VNZqzHBYazJx\/QBB3bG45cZSOvV3gUOnGBgiv9NBWjhvmY0fC3J6Q=="]}]}`,
			Iat:          int64(1513165889),
		},
		{
			Name:         "Valid Token with RS256 signature verified against correct JWKS document but predated",
			TokenString:  "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IllEQlVocWRXa3NLWGRHdVgwc3l0amFVdXhoQSIsImtpZCI6IllEQlVocWRXa3NLWGRHdVgwc3l0amFVdXhoQSJ9.eyJhdWQiOiJjaHJvbm9ncmFmIiwiaXNzIjoiaHR0cHM6Ly9kc3RjaW1hYWQxcC5kc3QtaXRzLmRlL2FkZnMiLCJpYXQiOjE1MTMxNjU4ODksImV4cCI6MTUxMzE2OTQ4OSwiYXV0aF90aW1lIjoxNTEzMTY1ODg4LCJzdWIiOiJlWVYzamRsZE55RlkxcUZGSDRvQWRCdkRGZmJWZm51RzI5SGlIa1N1andrPSIsInVwbiI6ImJzY0Bkc3QtaXRzLmRlIiwidW5pcXVlX25hbWUiOiJEU1RcXGJzYyIsInNpZCI6IlMtMS01LTIxLTI1MDUxNTEzOTgtMjY2MTAyODEwOS0zNzU0MjY1ODIwLTExMDQifQ.nK51Ui4XN45SVul9igNaKFQd-F63BNstBzW-T5LBVm_ANHCEHyP3_88C3ffkkQIi3PxYacRJGtfswP35ws7YJUcNp-GoGZARqz62NpMtbQyhos6mCaVXwPoxPbrZx4AkMQgxkZwJcOzceX7mpjcT3kCth30chN3lkhzSjGrXe4ZDOAV25liS-dsdBiqDiaTB91sS534GM76qJQxFUs51oSbYTRdCN1VJ0XopMcasfVDzFrtSbyvEIVXlpKK2HplnhheqF4QHrM_3cjV_NGRr3tYLe-AGTdDXKWlJD1GDz1ECXeMGQHPoz3U8cqNsFLYBstIlCgfnBWgWsPZSvJPJUg",
			JwksDocument: `{"keys":[{"kty":"RSA","use":"sig","alg":"RS256","kid":"YDBUhqdWksKXdGuX0sytjaUuxhA","x5t":"YDBUhqdWksKXdGuX0sytjaUuxhA","n":"uwVVrs5OJRKeLUk0H5N_b4Jbvff3rxlg3WIeOO-zSSPTC5oFOc5_te0rLgVoNJJB4rNM4A7BEXI885xLrjfL3l3LHqaJetvR0tdLAnkvbUKUiGxnuGnmOsgh491P95pHPIAniy2p64FQoBbTJ0a6cF5LRuPPHKVXgjXjTydvmKrt_IVaWUDgICRsw5Bbv290SahmxcdO3akSgfsZtRkR8SmaMzAPYINi2_8P2evaKAnMQLTgUVkctaEamO_6HJ5f5sWheV7trLekU35xPVkPwShDelefnhyJcO5yICXqXzuewBEni9LrxAEJYN2rYfiFQWJy-pDe5DPUBs-IFTpctQ","e":"AQAB","x5c":["MIIC6DCCAdCgAwIBAgIQPszqLhbrpZlE+jEJTyJg7jANBgkqhkiG9w0BAQsFADAwMS4wLAYDVQQDEyVBREZTIFNpZ25pbmcgLSBkc3RjaW1hYWQxcC5kc3QtaXRzLmRlMB4XDTE3MTIwNDE0MDEwOFoXDTE4MTIwNDE0MDEwOFowMDEuMCwGA1UEAxMlQURGUyBTaWduaW5nIC0gZHN0Y2ltYWFkMXAuZHN0LWl0cy5kZTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALsFVa7OTiUSni1JNB+Tf2+CW733968ZYN1iHjjvs0kj0wuaBTnOf7XtKy4FaDSSQeKzTOAOwRFyPPOcS643y95dyx6miXrb0dLXSwJ5L21ClIhsZ7hp5jrIIePdT\/eaRzyAJ4stqeuBUKAW0ydGunBeS0bjzxylV4I1408nb5iq7fyFWllA4CAkbMOQW79vdEmoZsXHTt2pEoH7GbUZEfEpmjMwD2CDYtv\/D9nr2igJzEC04FFZHLWhGpjv+hyeX+bFoXle7ay3pFN+cT1ZD8EoQ3pXn54ciXDuciAl6l87nsARJ4vS68QBCWDdq2H4hUFicvqQ3uQz1AbPiBU6XLUCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAPHCisZyPf\/fuuQEW5LyzZSYMwBRYVR6kk\/M2ZNx6TrUEwmOb10RQ3G97bLAshN44g5lWdPYz4EOt6d2o71etIjf79f+IR0MAjEgBB2HThaHcMU9KG229Ftcauie9XeurngMawTRu60YqH7+go8EMf6a1Kdnx37DMy\/1LRlsYJVfEoOCab3GgcIdXrRSYWqsY4SVJZiTPYdqz9vmNPSXXiDSOTl6qXHV\/f53WTS2V5aIQbuJJziXlceusuVNny0o5h+j6ovZ1HhEGAu3lpD+8kY8KUqA4kXMH3VNZqzHBYazJx\/QBB3bG45cZSOvV3gUOnGBgiv9NBWjhvmY0fC3J6Q=="]}]}`,
			Iat:          int64(1513165889) - 1,
			Err:          errors.New("Token used before issued"),
		},
		{
			Name:         "Valid Token with RS256 signature verified against correct JWKS document but outdated",
			TokenString:  "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IllEQlVocWRXa3NLWGRHdVgwc3l0amFVdXhoQSIsImtpZCI6IllEQlVocWRXa3NLWGRHdVgwc3l0amFVdXhoQSJ9.eyJhdWQiOiJjaHJvbm9ncmFmIiwiaXNzIjoiaHR0cHM6Ly9kc3RjaW1hYWQxcC5kc3QtaXRzLmRlL2FkZnMiLCJpYXQiOjE1MTMxNjU4ODksImV4cCI6MTUxMzE2OTQ4OSwiYXV0aF90aW1lIjoxNTEzMTY1ODg4LCJzdWIiOiJlWVYzamRsZE55RlkxcUZGSDRvQWRCdkRGZmJWZm51RzI5SGlIa1N1andrPSIsInVwbiI6ImJzY0Bkc3QtaXRzLmRlIiwidW5pcXVlX25hbWUiOiJEU1RcXGJzYyIsInNpZCI6IlMtMS01LTIxLTI1MDUxNTEzOTgtMjY2MTAyODEwOS0zNzU0MjY1ODIwLTExMDQifQ.nK51Ui4XN45SVul9igNaKFQd-F63BNstBzW-T5LBVm_ANHCEHyP3_88C3ffkkQIi3PxYacRJGtfswP35ws7YJUcNp-GoGZARqz62NpMtbQyhos6mCaVXwPoxPbrZx4AkMQgxkZwJcOzceX7mpjcT3kCth30chN3lkhzSjGrXe4ZDOAV25liS-dsdBiqDiaTB91sS534GM76qJQxFUs51oSbYTRdCN1VJ0XopMcasfVDzFrtSbyvEIVXlpKK2HplnhheqF4QHrM_3cjV_NGRr3tYLe-AGTdDXKWlJD1GDz1ECXeMGQHPoz3U8cqNsFLYBstIlCgfnBWgWsPZSvJPJUg",
			JwksDocument: `{"keys":[{"kty":"RSA","use":"sig","alg":"RS256","kid":"YDBUhqdWksKXdGuX0sytjaUuxhA","x5t":"YDBUhqdWksKXdGuX0sytjaUuxhA","n":"uwVVrs5OJRKeLUk0H5N_b4Jbvff3rxlg3WIeOO-zSSPTC5oFOc5_te0rLgVoNJJB4rNM4A7BEXI885xLrjfL3l3LHqaJetvR0tdLAnkvbUKUiGxnuGnmOsgh491P95pHPIAniy2p64FQoBbTJ0a6cF5LRuPPHKVXgjXjTydvmKrt_IVaWUDgICRsw5Bbv290SahmxcdO3akSgfsZtRkR8SmaMzAPYINi2_8P2evaKAnMQLTgUVkctaEamO_6HJ5f5sWheV7trLekU35xPVkPwShDelefnhyJcO5yICXqXzuewBEni9LrxAEJYN2rYfiFQWJy-pDe5DPUBs-IFTpctQ","e":"AQAB","x5c":["MIIC6DCCAdCgAwIBAgIQPszqLhbrpZlE+jEJTyJg7jANBgkqhkiG9w0BAQsFADAwMS4wLAYDVQQDEyVBREZTIFNpZ25pbmcgLSBkc3RjaW1hYWQxcC5kc3QtaXRzLmRlMB4XDTE3MTIwNDE0MDEwOFoXDTE4MTIwNDE0MDEwOFowMDEuMCwGA1UEAxMlQURGUyBTaWduaW5nIC0gZHN0Y2ltYWFkMXAuZHN0LWl0cy5kZTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALsFVa7OTiUSni1JNB+Tf2+CW733968ZYN1iHjjvs0kj0wuaBTnOf7XtKy4FaDSSQeKzTOAOwRFyPPOcS643y95dyx6miXrb0dLXSwJ5L21ClIhsZ7hp5jrIIePdT\/eaRzyAJ4stqeuBUKAW0ydGunBeS0bjzxylV4I1408nb5iq7fyFWllA4CAkbMOQW79vdEmoZsXHTt2pEoH7GbUZEfEpmjMwD2CDYtv\/D9nr2igJzEC04FFZHLWhGpjv+hyeX+bFoXle7ay3pFN+cT1ZD8EoQ3pXn54ciXDuciAl6l87nsARJ4vS68QBCWDdq2H4hUFicvqQ3uQz1AbPiBU6XLUCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAPHCisZyPf\/fuuQEW5LyzZSYMwBRYVR6kk\/M2ZNx6TrUEwmOb10RQ3G97bLAshN44g5lWdPYz4EOt6d2o71etIjf79f+IR0MAjEgBB2HThaHcMU9KG229Ftcauie9XeurngMawTRu60YqH7+go8EMf6a1Kdnx37DMy\/1LRlsYJVfEoOCab3GgcIdXrRSYWqsY4SVJZiTPYdqz9vmNPSXXiDSOTl6qXHV\/f53WTS2V5aIQbuJJziXlceusuVNny0o5h+j6ovZ1HhEGAu3lpD+8kY8KUqA4kXMH3VNZqzHBYazJx\/QBB3bG45cZSOvV3gUOnGBgiv9NBWjhvmY0fC3J6Q=="]}]}`,
			Iat:          int64(1513165889) + 3601,
			Err:          errors.New("Token is expired"),
		},
		{
			Name:         "Valid Token with RS256 signature verified against empty JWKS document",
			TokenString:  "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IllEQlVocWRXa3NLWGRHdVgwc3l0amFVdXhoQSIsImtpZCI6IllEQlVocWRXa3NLWGRHdVgwc3l0amFVdXhoQSJ9.eyJhdWQiOiJjaHJvbm9ncmFmIiwiaXNzIjoiaHR0cHM6Ly9kc3RjaW1hYWQxcC5kc3QtaXRzLmRlL2FkZnMiLCJpYXQiOjE1MTMxNjU4ODksImV4cCI6MTUxMzE2OTQ4OSwiYXV0aF90aW1lIjoxNTEzMTY1ODg4LCJzdWIiOiJlWVYzamRsZE55RlkxcUZGSDRvQWRCdkRGZmJWZm51RzI5SGlIa1N1andrPSIsInVwbiI6ImJzY0Bkc3QtaXRzLmRlIiwidW5pcXVlX25hbWUiOiJEU1RcXGJzYyIsInNpZCI6IlMtMS01LTIxLTI1MDUxNTEzOTgtMjY2MTAyODEwOS0zNzU0MjY1ODIwLTExMDQifQ.nK51Ui4XN45SVul9igNaKFQd-F63BNstBzW-T5LBVm_ANHCEHyP3_88C3ffkkQIi3PxYacRJGtfswP35ws7YJUcNp-GoGZARqz62NpMtbQyhos6mCaVXwPoxPbrZx4AkMQgxkZwJcOzceX7mpjcT3kCth30chN3lkhzSjGrXe4ZDOAV25liS-dsdBiqDiaTB91sS534GM76qJQxFUs51oSbYTRdCN1VJ0XopMcasfVDzFrtSbyvEIVXlpKK2HplnhheqF4QHrM_3cjV_NGRr3tYLe-AGTdDXKWlJD1GDz1ECXeMGQHPoz3U8cqNsFLYBstIlCgfnBWgWsPZSvJPJUg",
			JwksDocument: "",
			Iat:          int64(1513165889),
			Err:          errors.New("unexpected end of JSON input"),
		},
		{
			Name: "Invalid Token",
			Err:  errors.New("token contains an invalid number of segments"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			// mock JWKS server
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				io.WriteString(w, tt.JwksDocument)
			}))
			defer ts.Close()

			j := oauth2.JWT{
				Jwksurl: ts.URL,
				Now: func() time.Time {
					return time.Unix(tt.Iat, 0)
				},
			}
			_, err := j.GetClaims(tt.TokenString)
			if tt.Err != nil {
				if err != nil {
					if tt.Err.Error() != err.Error() {
						t.Errorf("Error in test %s expected error: %v actual: %v", tt.Name, tt.Err, err)
					} // else: that's what we expect
				} else {
					t.Errorf("Error in test %s expected error: %v actual: none", tt.Name, tt.Err)
				}
			} else {
				if err != nil {
					t.Errorf("Error in tt %s: %v", tt.Name, err)
				} // else: that's what we expect
			}
		})
	}
}

func TestJWT_ExtendedPrincipal(t *testing.T) {
	history := time.Unix(-446774400, 0)
	type fields struct {
		Now func() time.Time
	}
	type args struct {
		ctx       context.Context
		principal oauth2.Principal
		extension time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    oauth2.Principal
		wantErr bool
	}{
		{
			name: "Extend principal by one hour",
			fields: fields{
				Now: func() time.Time {
					return history
				},
			},
			args: args{
				ctx: context.Background(),
				principal: oauth2.Principal{
					ExpiresAt: history,
				},
				extension: time.Hour,
			},
			want: oauth2.Principal{
				ExpiresAt: history.Add(time.Hour),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &oauth2.JWT{
				Now: tt.fields.Now,
			}
			got, err := j.ExtendedPrincipal(tt.args.ctx, tt.args.principal, tt.args.extension)
			if (err != nil) != tt.wantErr {
				t.Errorf("JWT.ExtendedPrincipal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JWT.ExtendedPrincipal() = %v, want %v", got, tt.want)
			}
		})
	}
}
