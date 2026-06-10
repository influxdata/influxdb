use std::sync::Arc;

use influxdb3_authz::role::Role;
use influxdb3_id::RoleId;

use crate::Result;
use crate::catalog::Repository;
use crate::resource::CatalogResource;

impl CatalogResource for Role {
    type Identifier = RoleId;

    const CATEGORY: &'static str = "roles";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        self.name.as_str().into()
    }
}

#[derive(Debug, Clone, Default)]
pub struct RoleRepository {
    repo: Repository<RoleId, Role>,
}

impl RoleRepository {
    pub fn new(repo: Repository<RoleId, Role>) -> Self {
        Self { repo }
    }

    pub fn repo(&self) -> &Repository<RoleId, Role> {
        &self.repo
    }

    pub fn get_and_increment_next_id(&mut self) -> RoleId {
        self.repo.get_and_increment_next_id()
    }

    pub fn set_next_id(&mut self, id: RoleId) {
        self.repo.set_next_id(id);
    }

    pub fn get_by_id(&self, id: &RoleId) -> Option<Arc<Role>> {
        self.repo.get_by_id(id)
    }

    pub fn get_by_name(&self, name: &str) -> Option<Arc<Role>> {
        self.repo.get_by_name(name)
    }

    pub fn add_role(&mut self, role: Role) -> Result<()> {
        let id = role.id;
        Ok(self.repo.insert(id, role)?)
    }

    pub fn update_role(&mut self, role: Role) -> Result<()> {
        let id = role.id;
        Ok(self.repo.update(id, role)?)
    }

    pub fn remove_role(&mut self, id: &RoleId) {
        self.repo.remove(id);
    }

    pub fn iter(&self) -> impl Iterator<Item = (&RoleId, &Arc<Role>)> {
        self.repo.iter()
    }

    pub fn all_roles(&self) -> impl Iterator<Item = &Arc<Role>> {
        self.repo.resource_iter()
    }
}
