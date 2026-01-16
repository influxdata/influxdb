package authorization_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	influxdb2_algo "github.com/influxdata/influxdb/v2/pkg/crypt/algorithm/influxdb2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const (
	authIndexName       = "authorizationindexv1"
	hashedAuthIndexName = "authorizationhashedindexv1"
)

func TestAuth(t *testing.T) {
	const initialTokenCount = 10
	generateToken := func(i int) string { return fmt.Sprintf("randomtoken%d", i) }

	checkIndexCounts := func(t *testing.T, tx kv.Tx, expAuthIndexCount, expHashedAuthIndexCount int) {
		t.Helper()

		indexCount := make(map[string]int)
		for _, indexName := range []string{authIndexName, hashedAuthIndexName} {
			index, err := tx.Bucket([]byte(indexName))
			require.NoError(t, err)
			cur, err := index.Cursor()
			require.NoError(t, err)
			for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
				indexCount[indexName]++
			}
		}

		require.Equal(t, expAuthIndexCount, indexCount[authIndexName])
		require.Equal(t, expHashedAuthIndexCount, indexCount[hashedAuthIndexName])
	}

	setup := func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
		for i := 1; i <= initialTokenCount; i++ {
			err := store.CreateAuthorization(context.Background(), tx, &influxdb.Authorization{
				ID:     platform.ID(i),
				Token:  generateToken(i),
				OrgID:  platform.ID(i),
				UserID: platform.ID(i),
				Status: influxdb.Active,
			})
			require.NoError(t, err)
		}

		// Perform sanity checks on Token vs HashedToken and indices.
		for i := 1; i <= initialTokenCount; i++ {
			expToken := generateToken(i)
			a, err := store.GetAuthorizationByToken(context.Background(), tx, expToken)
			require.NoError(t, err)
			if useHashedTokens {
				require.Empty(t, a.Token)
				hashedToken, err := hasher.Hash(expToken)
				require.NoError(t, err)
				require.Equal(t, hashedToken, a.HashedToken)
			} else {
				require.Equal(t, expToken, a.Token)
				require.Empty(t, a.HashedToken)
			}
		}

		var expAuthIndexCount, expHashedAuthIndexCount int
		if useHashedTokens {
			expHashedAuthIndexCount = initialTokenCount
		} else {
			expAuthIndexCount = initialTokenCount
		}
		checkIndexCounts(t, tx, expAuthIndexCount, expHashedAuthIndexCount)
	}

	tt := []struct {
		name    string
		setup   func(*testing.T, bool, *authorization.Store, *authorization.AuthorizationHasher, kv.Tx)
		update  func(*testing.T, *authorization.Store, *authorization.AuthorizationHasher, kv.Tx)
		results func(*testing.T, bool, *authorization.Store, *authorization.AuthorizationHasher, kv.Tx)
	}{
		{
			name:  "create duplicate token",
			setup: setup,
			update: func(t *testing.T, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
				// should not be able to create two authorizations with identical tokens
				err := store.CreateAuthorization(context.Background(), tx, &influxdb.Authorization{
					ID:     platform.ID(1),
					Token:  generateToken(1),
					OrgID:  platform.ID(1),
					UserID: platform.ID(1),
				})
				require.ErrorIs(t, err, influxdb.ErrUnableToCreateToken)
			},
			results: func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
				auths, err := store.ListAuthorizations(context.Background(), tx, influxdb.AuthorizationFilter{})
				require.NoError(t, err)
				require.Len(t, auths, initialTokenCount)

				expected := []*influxdb.Authorization{}
				for i := 1; i <= initialTokenCount; i++ {
					a := &influxdb.Authorization{
						ID:     platform.ID(i),
						Token:  generateToken(i),
						OrgID:  platform.ID(i),
						UserID: platform.ID(i),
						Status: "active",
					}
					if useHashedTokens {
						hashedToken, err := hasher.Hash(a.Token)
						require.NoError(t, err)
						a.HashedToken = hashedToken
						a.ClearToken()
					}
					expected = append(expected, a)
				}
				require.Equal(t, auths, expected)

				var expAuthIndexCount, expHashedAuthIndexCount int
				if useHashedTokens {
					expHashedAuthIndexCount = initialTokenCount
				} else {
					expAuthIndexCount = initialTokenCount
				}
				checkIndexCounts(t, tx, expAuthIndexCount, expHashedAuthIndexCount)
			},
		},
		{
			name:  "read",
			setup: setup,
			results: func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
				for i := 1; i <= initialTokenCount; i++ {
					expectedAuth := &influxdb.Authorization{
						ID:     platform.ID(i),
						Token:  generateToken(i),
						OrgID:  platform.ID(i),
						UserID: platform.ID(i),
						Status: influxdb.Active,
					}
					if useHashedTokens {
						hashedToken, err := hasher.Hash(expectedAuth.Token)
						require.NoError(t, err)
						expectedAuth.HashedToken = hashedToken
						expectedAuth.ClearToken()
					}

					authByID, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					require.NoError(t, err)
					require.Equal(t, expectedAuth, authByID)

					authByToken, err := store.GetAuthorizationByToken(context.Background(), tx, generateToken(i))
					require.NoError(t, err)
					require.Equal(t, expectedAuth, authByToken)
				}

				var expAuthIndexCount, expHashedAuthIndexCount int
				if useHashedTokens {
					expHashedAuthIndexCount = initialTokenCount
				} else {
					expAuthIndexCount = initialTokenCount
				}
				checkIndexCounts(t, tx, expAuthIndexCount, expHashedAuthIndexCount)
			},
		},
		{
			name:  "update",
			setup: setup,
			update: func(t *testing.T, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
				for i := 1; i <= initialTokenCount; i++ {
					auth, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					require.NoError(t, err)

					auth.Status = influxdb.Inactive
					copyAuth := *auth

					updatedAuth, err := store.UpdateAuthorization(context.Background(), tx, platform.ID(i), auth)
					require.NoError(t, err)
					require.Equal(t, auth, updatedAuth) /* should be the same pointer */
					require.Equal(t, copyAuth, *auth)   /* should be the same contents */
				}
			},
			results: func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {

				for i := 1; i <= initialTokenCount; i++ {
					auth, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					require.NoError(t, err)

					expectedAuth := &influxdb.Authorization{
						ID:     platform.ID(i),
						Token:  generateToken(i),
						OrgID:  platform.ID(i),
						UserID: platform.ID(i),
						Status: influxdb.Inactive,
					}
					if useHashedTokens {
						hashedToken, err := hasher.Hash(expectedAuth.Token)
						require.NoError(t, err)
						expectedAuth.HashedToken = hashedToken
						expectedAuth.ClearToken()
					}

					require.Equal(t, expectedAuth, auth)
				}
				var expAuthIndexCount, expHashedAuthIndexCount int
				if useHashedTokens {
					expHashedAuthIndexCount = initialTokenCount
				} else {
					expAuthIndexCount = initialTokenCount
				}
				checkIndexCounts(t, tx, expAuthIndexCount, expHashedAuthIndexCount)
			},
		},
		{
			name:  "delete",
			setup: setup,
			update: func(t *testing.T, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
				for i := 1; i <= initialTokenCount; i++ {
					err := store.DeleteAuthorization(context.Background(), tx, platform.ID(i))
					require.NoError(t, err)
				}
			},
			results: func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
				for i := 1; i <= initialTokenCount; i++ {
					a, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					require.ErrorIs(t, err, authorization.ErrAuthNotFound)
					require.Nil(t, a)
				}
				checkIndexCounts(t, tx, 0, 0)
			},
		},
		{
			// This is an artificial test to set both Token and HashedToken. This should not occur in normal operation, but
			// we want to make sure we have the correct behavior.
			name:  "set Token and HashedToken",
			setup: setup,
			update: func(t *testing.T, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
				for i := 1; i <= initialTokenCount; i++ {
					auth, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					require.NoError(t, err)

					auth.Token = generateToken(i)
					hashedToken, err := hasher.Hash(auth.Token)
					require.NoError(t, err)
					auth.HashedToken = hashedToken

					newAuth, err := store.UpdateAuthorization(context.Background(), tx, platform.ID(i), auth)
					require.NoError(t, err)
					require.NotNil(t, newAuth)

					// Make sure update fails if tokens mismatch.
					auth.Token = "Hadouken"
					badHashedToken, err := hasher.Hash("Shoryuken")
					require.NoError(t, err)
					auth.HashedToken = badHashedToken
					newAuth, err = store.UpdateAuthorization(context.Background(), tx, platform.ID(i), auth)
					require.ErrorIs(t, err, authorization.ErrHashedTokenMismatch)
					require.Nil(t, newAuth)
				}
			},
			results: func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
				for i := 1; i <= initialTokenCount; i++ {
					authByID, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					require.NoError(t, err)
					if !useHashedTokens {
						require.Equal(t, generateToken(i), authByID.Token)
						require.Empty(t, authByID.HashedToken)
					} else {
						require.Empty(t, authByID.Token)
						hashedToken, err := hasher.Hash(generateToken(i))
						require.NoError(t, err)
						require.Equal(t, hashedToken, authByID.HashedToken)
					}

					// Should get the exact same record when fetching by the token.
					authByToken, err := store.GetAuthorizationByToken(context.Background(), tx, generateToken(i))
					require.NoError(t, err)
					require.Equal(t, *authByID, *authByToken)
				}

				if !useHashedTokens {
					// All unhashed index entries.
					checkIndexCounts(t, tx, initialTokenCount, 0)
				} else {
					// All hashed index entries.
					checkIndexCounts(t, tx, 0, initialTokenCount)
				}
			},
		},
	}

	for _, testScenario := range tt {
		for _, useHashedTokens := range []bool{false, true} {

			t.Run(testScenario.name, func(t *testing.T) {
				store := inmem.NewKVStore()
				err := all.Up(context.Background(), zaptest.NewLogger(t), store)
				require.NoError(t, err)

				hasher, err := authorization.NewAuthorizationHasher()
				require.NoError(t, err)
				require.NotNil(t, hasher)

				ts, err := authorization.NewStore(context.Background(), store, useHashedTokens, authorization.WithAuthorizationHasher(hasher))
				require.NoError(t, err)
				require.NotNil(t, ts)

				// setup
				if testScenario.setup != nil {
					err := ts.Update(context.Background(), func(tx kv.Tx) error {
						testScenario.setup(t, useHashedTokens, ts, hasher, tx)
						return nil
					})
					require.NoError(t, err)
				}

				// update
				if testScenario.update != nil {
					err := ts.Update(context.Background(), func(tx kv.Tx) error {
						testScenario.update(t, ts, hasher, tx)
						return nil
					})
					require.NoError(t, err)
				}

				// results
				if testScenario.results != nil {
					err := ts.View(context.Background(), func(tx kv.Tx) error {
						testScenario.results(t, useHashedTokens, ts, hasher, tx)
						return nil
					})
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestAuthorizationStore_HashingConfigChanges(t *testing.T) {
	sha256, err := influxdb2_algo.New(influxdb2_algo.WithVariant(influxdb2_algo.VariantSHA256))
	require.NoError(t, err)
	sha512, err := influxdb2_algo.New(influxdb2_algo.WithVariant(influxdb2_algo.VariantSHA512))
	require.NoError(t, err)

	type authData struct {
		ID          platform.ID
		Token       string
		HashedToken string
	}
	type testConfig struct {
		enabled bool
		algo    string
	}
	type testCase struct {
		desc         string
		config       testConfig
		action       func(t *testing.T, ctx context.Context, store *authorization.Store, tx kv.Tx)
		exp          []authData
		hashedTokens []string // tokens which only exists as hashes
	}
	cases := []testCase{
		{
			desc:   "initial unhashed",
			config: testConfig{enabled: false},
			action: func(t *testing.T, ctx context.Context, store *authorization.Store, tx kv.Tx) {
				a := &influxdb.Authorization{
					ID:     platform.ID(1),
					OrgID:  platform.ID(1),
					UserID: platform.ID(1),
					Token:  "Token#1",
				}
				require.NoError(t, store.CreateAuthorization(ctx, tx, a))
			},
			exp: []authData{
				{ID: platform.ID(1), Token: "Token#1"},
			},
		},
		{
			desc:   "upgrade hashed #1", // update hash and indices
			config: testConfig{enabled: true, algo: influxdb2_algo.VariantIdentifierSHA256},
			exp: []authData{
				{ID: platform.ID(1), HashedToken: sha256.MustHash("Token#1").Encode()},
			},
			hashedTokens: []string{"Token#1"},
		},
		{
			desc:   "downgrade hashed #1", // can't unhash
			config: testConfig{enabled: false, algo: influxdb2_algo.VariantIdentifierSHA256},
			action: func(t *testing.T, ctx context.Context, store *authorization.Store, tx kv.Tx) {
				a := &influxdb.Authorization{
					ID:     platform.ID(2),
					OrgID:  platform.ID(2),
					UserID: platform.ID(2),
					Token:  "Token#2",
				}
				require.NoError(t, store.CreateAuthorization(ctx, tx, a))
			},
			exp: []authData{
				{ID: platform.ID(1), HashedToken: sha256.MustHash("Token#1").Encode()},
				{ID: platform.ID(2), Token: "Token#2"},
			},
			hashedTokens: []string{"Token#1"},
		},
		{
			desc:   "upgrade hashed sha512", // can't rehash existing, use new algo for new auths
			config: testConfig{enabled: true, algo: influxdb2_algo.VariantIdentifierSHA512},
			action: func(t *testing.T, ctx context.Context, store *authorization.Store, tx kv.Tx) {
				a := &influxdb.Authorization{
					ID:     platform.ID(3),
					OrgID:  platform.ID(3),
					UserID: platform.ID(3),
					Token:  "Token#3",
				}
				require.NoError(t, store.CreateAuthorization(ctx, tx, a))
			},
			exp: []authData{
				{ID: platform.ID(1), HashedToken: sha256.MustHash("Token#1").Encode()},
				{ID: platform.ID(2), HashedToken: sha512.MustHash("Token#2").Encode()},
				{ID: platform.ID(3), HashedToken: sha512.MustHash("Token#3").Encode()},
			},
			hashedTokens: []string{"Token#1", "Token#2", "Token#3"},
		},

		// The following tests are artificial tests intended to check proper operation when both
		// Token and HashedToken are set on an update. This should not occur in normal operation because,
		// we do not alter tokens like this. However, there is nothing to prevent this so we want to make sure
		// it works properly.
		{
			desc:   "set Token and HashedToken with hashing enabled",
			config: testConfig{enabled: true, algo: influxdb2_algo.VariantIdentifierSHA512},
			action: func(t *testing.T, ctx context.Context, store *authorization.Store, tx kv.Tx) {
				for i := 1; i <= 3; i++ {
					token := fmt.Sprintf("Token#%d", i)
					auth, err := store.GetAuthorizationByToken(ctx, tx, token)
					require.NoError(t, err)
					require.Empty(t, auth.Token, "only HashedToken should be stored")

					// Set Token and update.
					auth.Token = token
					newAuth, err := store.UpdateAuthorization(ctx, tx, platform.ID(i), auth)
					require.NoError(t, err)

					// newAuth.Token should not have been saved to BoltDB, but newAuth.Token should still be present
					require.Equal(t, token, newAuth.Token)
				}
			},
			// NOTE: All hashes should be updated to the currently configured algorithm.
			exp: []authData{
				{ID: platform.ID(1), HashedToken: sha512.MustHash("Token#1").Encode()},
				{ID: platform.ID(2), HashedToken: sha512.MustHash("Token#2").Encode()},
				{ID: platform.ID(3), HashedToken: sha512.MustHash("Token#3").Encode()},
			},
			hashedTokens: []string{"Token#1", "Token#2", "Token#3"},
		},
		{
			desc:   "set Token and HashedToken with hashing disabled",
			config: testConfig{enabled: false},
			action: func(t *testing.T, ctx context.Context, store *authorization.Store, tx kv.Tx) {
				for i := 1; i <= 3; i++ {
					token := fmt.Sprintf("Token#%d", i)
					auth, err := store.GetAuthorizationByToken(ctx, tx, token)
					require.NoError(t, err)
					require.Empty(t, auth.Token, "only HashedToken should be stored")
					require.NotEmpty(t, auth.HashedToken, "HashedToken should be set")

					// Set Token and update.
					auth.Token = token
					newAuth, err := store.UpdateAuthorization(ctx, tx, platform.ID(i), auth)
					require.NoError(t, err)

					// newAuth.Token should be set, but newAuth.HashedToken should be cleared.
					require.Equal(t, token, newAuth.Token)
					require.Empty(t, newAuth.HashedToken)
				}
			},
			exp: []authData{
				{ID: platform.ID(1), Token: "Token#1"},
				{ID: platform.ID(2), Token: "Token#2"},
				{ID: platform.ID(3), Token: "Token#3"},
			},
			hashedTokens: []string{},
		},
	}

	ctx := context.Background()

	// The underlying kv store persists across tests cases. This allows for testing how opening with
	// new authentication configurations impacts the data.
	kvStore := inmem.NewKVStore()
	err = all.Up(ctx, zaptest.NewLogger(t), kvStore)
	require.NoError(t, err)

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			// Create new authorization.Store for test cases using existing kvStore.
			variantName := tc.config.algo
			if variantName == "" {
				if !tc.config.enabled {
					variantName = authorization.DefaultHashVariantName
				} else {
					require.Fail(t, "Must specific algo if hashing is enabled for test case")
				}
			}
			store, err := authorization.NewStore(ctx, kvStore, tc.config.enabled, authorization.WithAuthorizationHashVariantName(variantName))
			require.NoError(t, err)
			require.NotNil(t, store)

			// Execute action, if given. Simply opening the store with a different configuration may be the "action".
			if tc.action != nil {
				err = kvStore.Update(ctx, func(tx kv.Tx) error {
					tc.action(t, ctx, store, tx)
					return nil
				})
				require.NoError(t, err)
			}

			// Check results.
			err = kvStore.View(ctx, func(tx kv.Tx) error {
				// Collect all authorization data from store.
				storedAuths, err := store.ListAuthorizations(ctx, tx, influxdb.AuthorizationFilter{})
				require.NoError(t, err)

				// Collect auth data from data currently in store
				actualAuthData := make([]authData, 0, len(storedAuths))
				for _, sa := range storedAuths {
					ad := authData{ID: sa.ID, Token: sa.Token, HashedToken: sa.HashedToken}
					actualAuthData = append(actualAuthData, ad)
				}

				// Check that authData matches exp.
				require.ElementsMatch(t, tc.exp, actualAuthData)

				// Collect data from kvStore's token index.
				collectIndex := func(indexName string) map[string]platform.ID {
					indexMap := make(map[string]platform.ID)
					index, err := tx.Bucket([]byte(indexName))
					require.NoError(t, err)
					cursor, err := index.Cursor()
					require.NoError(t, err)
					for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
						var id platform.ID
						require.NoError(t, id.Decode(v))
						indexMap[string(k)] = id
					}
					return indexMap
				}
				actualTokenIndex := collectIndex(authIndexName)
				actualHashedIndex := collectIndex(hashedAuthIndexName)

				// Collect expected token and hashed indices.
				expTokenIndex := make(map[string]platform.ID)
				expHashedIndex := make(map[string]platform.ID)
				for _, d := range tc.exp {
					if d.Token != "" {
						expTokenIndex[d.Token] = d.ID
					}
					if d.HashedToken != "" {
						expHashedIndex[d.HashedToken] = d.ID
					}
				}

				// Compare indices.
				require.Equal(t, expTokenIndex, actualTokenIndex)
				require.Equal(t, expHashedIndex, actualHashedIndex)

				// Make sure we can lookup all tokens.
				var allTokens []string
				for _, d := range tc.exp {
					if d.Token != "" {
						allTokens = append(allTokens, d.Token)
					}
				}
				allTokens = append(allTokens, tc.hashedTokens...)

				for _, token := range allTokens {
					auth, err := store.GetAuthorizationByToken(ctx, tx, token)
					require.NoError(t, err, "error looking up token %q", token)
					require.NotNil(t, auth)
				}

				return nil
			})
			require.NoError(t, err)
		})

	}
}

func TestNewStore_WithSkipTokenMigration(t *testing.T) {
	ctx := context.Background()

	// Create a kv store and run migrations.
	kvStore := inmem.NewKVStore()
	err := all.Up(ctx, zaptest.NewLogger(t), kvStore)
	require.NoError(t, err)

	// Create store with hashing disabled to populate raw tokens.
	store, err := authorization.NewStore(ctx, kvStore, false)
	require.NoError(t, err)

	// Create some authorizations with raw tokens.
	rawTokens := []string{"rawToken1", "rawToken2", "rawToken3"}
	err = kvStore.Update(ctx, func(tx kv.Tx) error {
		for i, token := range rawTokens {
			err := store.CreateAuthorization(ctx, tx, &influxdb.Authorization{
				ID:     platform.ID(i + 1),
				Token:  token,
				OrgID:  platform.ID(i + 1),
				UserID: platform.ID(i + 1),
				Status: influxdb.Active,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Verify tokens are stored as raw.
	err = kvStore.View(ctx, func(tx kv.Tx) error {
		for i, token := range rawTokens {
			auth, err := store.GetAuthorizationByID(ctx, tx, platform.ID(i+1))
			require.NoError(t, err)
			require.Equal(t, token, auth.Token, "token should be stored as raw")
			require.Empty(t, auth.HashedToken, "HashedToken should be empty")
		}
		return nil
	})
	require.NoError(t, err)

	// Now create a new store with hashing enabled but skip migration.
	storeWithSkip, err := authorization.NewStore(ctx, kvStore, true, authorization.WithSkipTokenMigration(true))
	require.NoError(t, err)
	require.NotNil(t, storeWithSkip)

	// Verify tokens are still stored as raw (migration was skipped).
	err = kvStore.View(ctx, func(tx kv.Tx) error {
		for i, token := range rawTokens {
			auth, err := storeWithSkip.GetAuthorizationByID(ctx, tx, platform.ID(i+1))
			require.NoError(t, err)
			require.Equal(t, token, auth.Token, "token should still be raw after skipped migration")
			require.Empty(t, auth.HashedToken, "HashedToken should still be empty after skipped migration")
		}
		return nil
	})
	require.NoError(t, err)

	// Also verify the raw tokens can still be looked up (raw index should still work).
	err = kvStore.View(ctx, func(tx kv.Tx) error {
		for i, token := range rawTokens {
			auth, err := storeWithSkip.GetAuthorizationByToken(ctx, tx, token)
			require.NoError(t, err)
			require.Equal(t, platform.ID(i+1), auth.ID)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestNewStore_WithForceAllVariants(t *testing.T) {
	ctx := context.Background()

	// Create a kv store and run migrations.
	kvStore := inmem.NewKVStore()
	err := all.Up(ctx, zaptest.NewLogger(t), kvStore)
	require.NoError(t, err)

	// Create both stores on an EMPTY store BEFORE any tokens exist.
	// This is critical: findHashVariants returns nothing on empty store.

	// Store with SHA256 variant, without ForceAllVariants - only SHA256 decoder available.
	storeSHA256Only, err := authorization.NewStore(ctx, kvStore, true,
		authorization.WithAuthorizationHashVariantName(influxdb2_algo.VariantIdentifierSHA256),
	)
	require.NoError(t, err)

	// Store with SHA256 variant, WITH ForceAllVariants - all decoders available.
	storeWithAllVariants, err := authorization.NewStore(ctx, kvStore, true,
		authorization.WithForceAllVariants(true),
		authorization.WithAuthorizationHashVariantName(influxdb2_algo.VariantIdentifierSHA256),
	)
	require.NoError(t, err)

	// Now create a third store with SHA512 variant to add a SHA512 hashed token.
	storeSHA512, err := authorization.NewStore(ctx, kvStore, true,
		authorization.WithAuthorizationHashVariantName(influxdb2_algo.VariantIdentifierSHA512),
		authorization.WithForceAllVariants(true),
	)
	require.NoError(t, err)

	// Create a token using the SHA512 store - it will be hashed with SHA512.
	token := "testTokenForVariants"
	err = kvStore.Update(ctx, func(tx kv.Tx) error {
		return storeSHA512.CreateAuthorization(ctx, tx, &influxdb.Authorization{
			ID:     platform.ID(1),
			Token:  token,
			OrgID:  platform.ID(1),
			UserID: platform.ID(1),
			Status: influxdb.Active,
		})
	})
	require.NoError(t, err)

	// Verify the token was hashed with SHA512.
	sha512Algo, err := influxdb2_algo.New(influxdb2_algo.WithVariant(influxdb2_algo.VariantSHA512))
	require.NoError(t, err)
	expectedSHA512Hash := sha512Algo.MustHash(token).Encode()

	err = kvStore.View(ctx, func(tx kv.Tx) error {
		auth, err := storeSHA512.GetAuthorizationByID(ctx, tx, platform.ID(1))
		require.NoError(t, err)
		require.Equal(t, expectedSHA512Hash, auth.HashedToken, "token should be hashed with SHA512")
		return nil
	})
	require.NoError(t, err)

	// storeSHA256Only was created on empty store without ForceAllVariants,
	// so it only has SHA256 decoder. It should NOT find the SHA512 hashed token.
	err = kvStore.View(ctx, func(tx kv.Tx) error {
		_, err := storeSHA256Only.GetAuthorizationByToken(ctx, tx, token)
		require.Error(t, err, "store with only SHA256 decoder should not find SHA512 hashed token")
		return nil
	})
	require.NoError(t, err)

	// storeWithAllVariants was created on empty store WITH ForceAllVariants,
	// so it has all decoders including SHA512. It SHOULD find the token.
	err = kvStore.View(ctx, func(tx kv.Tx) error {
		auth, err := storeWithAllVariants.GetAuthorizationByToken(ctx, tx, token)
		require.NoError(t, err, "store with ForceAllVariants should find SHA512 hashed token")
		require.Equal(t, platform.ID(1), auth.ID)
		return nil
	})
	require.NoError(t, err)
}
