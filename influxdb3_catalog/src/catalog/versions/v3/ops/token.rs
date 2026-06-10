//! Token operations: CreateAdminToken, RegenerateAdminToken, DeleteToken,
//! CreateResourceScopedToken.

use std::sync::Arc;

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::RecordBatch;
use crate::format::records::types::Permission as WirePermission;
use crate::format::records::{CreateAdminToken, DeleteToken, RegenerateAdminToken};
use influxdb3_authz::TokenInfo;
use influxdb3_id::TokenId;

// ---------------------------------------------------------------------------
// CreateAdminToken
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct CreateAdminTokenArgs {
    pub name: String,
    pub hash: Vec<u8>,
    pub created_at: i64,
    pub updated_at: Option<i64>,
    pub expiry: Option<i64>,
    pub description: Option<String>,
    pub created_by: Option<TokenId>,
    pub updated_by: Option<TokenId>,
}

pub(crate) struct CreateAdminTokenOp {
    token_id: TokenId,
}

impl CatalogOp for CreateAdminTokenOp {
    type Input = CreateAdminTokenArgs;
    type Output = Arc<TokenInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        if catalog.tokens.repo().get_by_name(&args.name).is_some() {
            return Err(CatalogError::TokenNameAlreadyExists(args.name.clone()));
        }

        let token_id = catalog.tokens.repo().next_id();
        records.push(&CreateAdminToken {
            token_id: token_id.get(),
            name: args.name.clone(),
            hash: args.hash.clone(),
            created_at: args.created_at,
            updated_at: args.updated_at,
            expiry: args.expiry,
            description: args.description.clone(),
            created_by: args.created_by.map(|id| id.get()),
            updated_by: args.updated_by.map(|id| id.get()),
        });

        Ok(Self { token_id })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .tokens
            .repo()
            .get_by_id(&self.token_id)
            .expect("token should exist after apply")
    }
}

// ---------------------------------------------------------------------------
// RegenerateAdminToken
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct RegenerateAdminTokenArgs {
    pub token_id: TokenId,
    pub new_hash: Vec<u8>,
    pub updated_at: i64,
}

pub(crate) struct RegenerateAdminTokenOp {
    token_id: TokenId,
}

impl CatalogOp for RegenerateAdminTokenOp {
    type Input = RegenerateAdminTokenArgs;
    type Output = Arc<TokenInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        if catalog.tokens.repo().get_by_id(&args.token_id).is_none() {
            return Err(CatalogError::MissingAdminTokenToUpdate);
        }

        records.push(&RegenerateAdminToken {
            token_id: args.token_id.get(),
            hash: args.new_hash.clone(),
            updated_at: Some(args.updated_at),
        });

        Ok(Self {
            token_id: args.token_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .tokens
            .repo()
            .get_by_id(&self.token_id)
            .expect("token should exist after regeneration")
    }
}

// ---------------------------------------------------------------------------
// DeleteToken
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct DeleteTokenArgs {
    pub token_name: String,
}

pub(crate) struct DeleteTokenOp {
    token: Arc<TokenInfo>,
}

impl CatalogOp for DeleteTokenOp {
    type Input = DeleteTokenArgs;
    type Output = Arc<TokenInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let token = catalog
            .tokens
            .repo()
            .get_by_name(&args.token_name)
            .ok_or_else(|| CatalogError::NotFound(args.token_name.clone()))?;

        records.push(&DeleteToken {
            token_id: token.id.get(),
        });

        Ok(Self { token })
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {
        Arc::clone(&self.token)
    }
}

// ---------------------------------------------------------------------------
// CreateResourceScopedToken
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct CreateResourceScopedTokenArgs {
    pub name: String,
    pub hash: Vec<u8>,
    pub created_at: i64,
    pub updated_at: Option<i64>,
    pub expiry: Option<i64>,
    pub description: Option<String>,
    pub created_by: Option<TokenId>,
    pub updated_by: Option<TokenId>,
    pub permissions: Vec<WirePermission>,
}

pub(crate) struct CreateResourceScopedTokenOp {
    token_id: TokenId,
}

impl CatalogOp for CreateResourceScopedTokenOp {
    type Input = CreateResourceScopedTokenArgs;
    type Output = Arc<TokenInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        if catalog.tokens.repo().get_by_name(&args.name).is_some() {
            return Err(CatalogError::TokenNameAlreadyExists(args.name.clone()));
        }

        let token_id = catalog.tokens.repo().next_id();
        records.push(
            &crate::enterprise::format::records::CreateResourceScopedToken {
                token_id: token_id.get(),
                name: args.name.clone(),
                hash: args.hash.clone(),
                created_at: args.created_at,
                updated_at: args.updated_at,
                expiry: args.expiry,
                description: args.description.clone(),
                created_by: args.created_by.map(|id| id.get()),
                updated_by: args.updated_by.map(|id| id.get()),
                permissions: args.permissions.clone(),
            },
        );

        Ok(Self { token_id })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .tokens
            .repo()
            .get_by_id(&self.token_id)
            .expect("token should exist after apply")
    }
}

#[cfg(test)]
mod tests;
