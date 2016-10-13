package handlers_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/influxdata/mrfusion/handlers"
)

func TestAuthorizedToken(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	req, _ := http.NewRequest("GET", "", nil)
	w := httptest.NewRecorder()
	handler := handlers.AuthorizedToken(handlers.JWTOpts{}, next)
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Error("Should have been unauthorized")
	}

	secret := "secret"
	// Create a new token object, specifying signing method and the claims
	// you would like it to contain.
	token := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
		"sub": "bob",
		"exp": time.Now().Add(10 * time.Second).Unix(),
	})

	// Sign and get the complete encoded token as a string using the secret
	tokenString, _ := token.SignedString([]byte(secret))

	req, _ = http.NewRequest("GET", "", nil)
	req.Header.Add("Authorization", "Bearer "+tokenString)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Error(fmt.Sprintf("Returned code of %d instead of 200", w.Code))
	}

	token = jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
		"sub": "bob",
		"exp": time.Now().Add(-10 * time.Second).Unix(),
	})

	tokenString, _ = token.SignedString([]byte(secret))

	req, _ = http.NewRequest("GET", "", nil)
	req.Header.Add("Authorization", "Bearer "+tokenString)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Error("Stale authorization should be rejected")
	}
}
