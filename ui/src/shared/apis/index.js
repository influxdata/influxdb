import AJAX from 'utils/ajax';

export function getSources() {
  return AJAX({
    url: '/chronograf/v1/sources',
  });
}

export function getSource(sourceID) {
  return AJAX({
    url: `/chronograf/v1/sources/${sourceID}`,
  });
}

export function createSource(attributes) {
  return AJAX({
    url: '/chronograf/v1/sources',
    method: 'POST',
    data: attributes,
  });
}

export function updateSource(newSource) {
  return AJAX({
    url: newSource.links.self,
    method: 'PATCH',
    data: newSource,
  });
}

export function updateCluster(clusterID, displayName) {
  return AJAX({
    url: `/api/int/v1/clusters/${clusterID}`,
    method: 'PUT',
    data: {
      display_name: displayName,
    },
  });
}

export function getDatabaseManager(clusterID, dbName) {
  return AJAX({
    url: `/api/int/v1/${clusterID}/databases/${dbName}`,
  });
}

export function createDatabase({database, rpName, duration, replicaN}) {
  const params = new window.URLSearchParams();
  params.append('name', database);
  params.append('retention-policy', rpName);
  params.append('duration', duration);
  params.append('replication-factor', replicaN);

  return AJAX({
    url: `/api/int/v1/databases`,
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    data: params,
  });
}

export function getClusters() {
  return AJAX({
    url: ``,
  });
}

export function meShow() {
  return AJAX({
    url: `/api/int/v1/me`,
  });
}

export function meUpdate({firstName, lastName, email, password, confirmation, oldPassword}) {
  return AJAX({
    url: `/api/int/v1/me`,
    method: 'PUT',
    data: {
      first_name: firstName,
      last_name: lastName,
      email,
      password,
      confirmation,
      old_password: oldPassword,
    },
  });
}

export function getWebUsers() {
  return AJAX({
    url: `/api/int/v1/users`,
  });
}

export function createWebUser({firstName, lastName, email, password}) {
  return AJAX({
    url: `/api/int/v1/users`,
    method: 'POST',
    data: {
      first_name: firstName,
      last_name: lastName,
      email,
      password,
    },
  });
}

export function deleteWebUsers(userID) {
  return AJAX({
    url: `/api/int/v1/users/${userID}`,
    method: 'DELETE',
  });
}

export function showUser(userID) {
  return AJAX({
    url: `/api/int/v1/users/${userID}`,
  });
}

export function updateUser(userID, {firstName, lastName, email, password, confirmation, admin}) {
  return AJAX({
    url: `/api/int/v1/users/${userID}`,
    method: 'PUT',
    data: {
      first_name: firstName,
      last_name: lastName,
      email,
      password,
      confirmation,
      admin,
    },
  });
}

export function getClusterAccounts(clusterID) {
  return AJAX({
    url: metaProxy(clusterID, '/user'),
  });
}

// can only be used for initial app setup.  will create first cluster user
// with global admin permissions.
export function createClusterUserAtSetup(clusterID, username, password) {
  return AJAX({
    url: `/api/v1/setup/cluster_user`,
    method: 'POST',
    data: {
      cluster_id: clusterID,
      username,
      password,
    },
  });
}

// can only be used for initial app setup
export function createWebAdmin({firstName, lastName, email, password, confirmation, clusterLinks}) {
  return AJAX({
    url: `/api/v1/setup/admin`,
    method: 'POST',
    data: {
      first_name: firstName,
      last_name: lastName,
      email,
      password,
      confirmation,
      cluster_links: clusterLinks,
    },
  });
}

// can only be used for initial app setup
export function updateClusterAtSetup(clusterID, displayName) {
  return AJAX({
    url: `/api/v1/setup/clusters/${clusterID}`,
    method: 'POST',
    data: {
      display_name: displayName,
    },
  });
}

export function createClusterUser(clusterID, name, password) {
  return AJAX({
    url: metaProxy(clusterID, '/user'),
    method: 'POST',
    data: {
      action: 'create',
      user: {
        name,
        password,
      },
    },
  });
}

export function addUsersToRole(clusterID, name, users) {
  return AJAX({
    url: metaProxy(clusterID, '/role'),
    method: 'POST',
    data: {
      action: 'add-users',
      role: {
        name,
        users,
      },
    },
  });
}

export function getClusterAccount(clusterID, accountID) {
  return AJAX({
    url: metaProxy(clusterID, `/user?name=${encodeURIComponent(accountID)}`),
  });
}

export function updateClusterAccountPassword(clusterID, name, password) {
  return AJAX({
    url: metaProxy(clusterID, '/user'),
    method: 'POST',
    data: {
      action: 'change-password',
      user: {
        name,
        password,
      },
    },
  });
}


export function getRoles(clusterID) {
  return AJAX({
    url: metaProxy(clusterID, '/role'),
  });
}

export function createRole(clusterID, roleName) {
  return AJAX({
    url: metaProxy(clusterID, '/role'),
    method: 'POST',
    data: {
      action: 'create',
      role: {
        name: roleName,
      },
    },
  });
}

// TODO: update usage on index page
export function deleteClusterAccount(clusterID, accountName) {
  return Promise.all([
    // Remove the cluster account from plutonium.
    AJAX({
      url: metaProxy(clusterID, '/user'),
      method: `POST`,
      data: {
        action: 'delete',
        user: {
          name: accountName,
        },
      },
    }),
    // Remove any cluster user links that are tied to this cluster account.
    AJAX({
      url: `/api/int/v1/user_links/batch/${accountName}`,
      method: 'DELETE',
    }),
  ]);
}

export function createClusterAccount(clusterID, name, password) {
  return AJAX({
    url: metaProxy(clusterID, '/user'),
    method: `POST`,
    data: {
      action: 'create',
      user: {
        name,
        password,
      },
    },
  });
}

export function addAccountsToRole(clusterID, roleName, usernames) {
  return AJAX({
    url: metaProxy(clusterID, '/role'),
    method: 'POST',
    data: {
      action: 'add-users',
      role: {
        name: roleName,
        users: usernames,
      },
    },
  });
}

export function removeAccountsFromRole(clusterID, roleName, usernames) {
  return AJAX({
    url: metaProxy(clusterID, '/role'),
    method: 'POST',
    data: {
      action: 'remove-users',
      role: {
        name: roleName,
        users: usernames,
      },
    },
  });
}

export function addPermissionToRole(clusterID, roleName, permission) {
  const permissions = buildPermissionForPlutonium(permission);
  return AJAX({
    url: metaProxy(clusterID, '/role'),
    method: 'POST',
    data: {
      action: 'add-permissions',
      role: {
        name: roleName,
        permissions,
      },
    },
  });
}

export function removePermissionFromRole(clusterID, roleName, permission) {
  const permissions = buildPermissionForPlutonium(permission);
  return AJAX({
    url: metaProxy(clusterID, '/role'),
    method: 'POST',
    data: {
      action: 'remove-permissions',
      role: {
        name: roleName,
        permissions,
      },
    },
  });
}

export function removePermissionFromAccount(clusterID, username, permission) {
  const permissions = buildPermissionForPlutonium(permission);
  return AJAX({
    url: metaProxy(clusterID, '/user'),
    method: 'POST',
    data: {
      action: 'remove-permissions',
      user: {
        name: username,
        permissions,
      },
    },
  });
}

// The structure that plutonium expects for adding permissions is a little unorthodox,
// where the permission(s) being added have to be under a resource key, e.g.
// {
//   "db1": ["ViewAdmin"],
//   "": ["CreateRole"]
// }
// This transforms a more web client-friendly permissions object into something plutonium understands.
function buildPermissionForPlutonium({name, resources}) {
  return resources.reduce((obj, resource) => {
    obj[resource] = [name];
    return obj;
  }, {});
}

export function addPermissionToAccount(clusterID, name, permission, resources) {
  const permissions = resources.reduce((obj, resource) => {
    obj[resource] = [permission];
    return obj;
  }, {});

  return AJAX({
    url: metaProxy(clusterID, '/user'),
    method: 'POST',
    data: {
      action: 'add-permissions',
      user: {
        name,
        permissions,
      },
    },
  });
}

export function deleteRole(clusterID, roleName) {
  return AJAX({
    url: metaProxy(clusterID, '/role'),
    method: 'POST',
    data: {
      action: 'delete',
      role: {
        name: roleName,
      },
    },
  });
}

export function deleteUserClusterLink(clusterID, userClusterLinkID) {
  return AJAX({
    url: `/api/int/v1/user_links/${userClusterLinkID}`,
    method: `DELETE`,
  });
}

export function getUserClusterLinks() {
  return AJAX({
    url: `/api/int/v1/user_links`,
  });
}

export function createUserClusterLink({userID, clusterID, clusterUser}) {
  return AJAX({
    url: `/api/int/v1/user_links`,
    method: 'POST',
    data: {
      user_id: userID,
      cluster_user: clusterUser,
      cluster_id: clusterID,
    },
  });
}

export function getWebUsersByClusterAccount(clusterID, clusterAccount) {
  return AJAX({
    url: `/api/int/v1/user_links/batch/${encodeURIComponent(clusterAccount)}`,
  });
}

export function batchCreateUserClusterLink(userID, clusterLinks) {
  return AJAX({
    url: `/api/int/v1/users/${userID}/cluster_links/batch`,
    method: 'POST',
    data: clusterLinks,
  });
}

export function addWebUsersToClusterAccount(clusterID, clusterAccount, userIDs) {
  return AJAX({
    url: `/api/int/v1/user_links/batch/${encodeURIComponent(clusterAccount)}`,
    method: 'POST',
    data: userIDs,
  });
}

function metaProxy(clusterID, slug) {
  return `/api/int/v1/meta${slug}`;
}

// Kapacitor functions
// TODO: update kapacitor functions to assume only one kapacitor. waiting for @goller

export function getKapacitor(source) {
  return AJAX({
    url: source.links.kapacitors,
    method: 'GET',
  }).then(({data}) => {
    return data.kapacitors[0];
  });
}

export function createKapacitor(source, {url, name = 'My Kapacitor', username, password}) {
  return AJAX({
    url: source.links.kapacitors,
    method: 'POST',
    data: {
      name,
      url,
      username,
      password,
    },
  });
}

export function updateKapacitor(kapacitor, {url, name = 'My Kapacitor', username, password}) {
  return AJAX({
    url: kapacitor.links.self,
    method: 'PATCH',
    data: {
      name,
      url,
      username,
      password,
    },
  });
}

export function getKapacitorConfig(kapacitor) {
  return kapacitorProxy(kapacitor, 'GET', '/kapacitor/v1/config', '');
}

// updateKapacitorConfigSection will update one section in the Kapacitor config.
export function updateKapacitorConfigSection(kapacitor, section, properties) {
  return AJAX({
    method: 'POST',
    url: kapacitor.links.proxy,
    params: {
      path: `/kapacitor/v1/config/${section}/`,
    },
    data: {
      set: properties,
    },
    headers: {
      'Content-Type': 'application/json',
    },
  });
}

export function testAlertOutput(kapacitor, outputName, properties) {
  return kapacitorProxy(kapacitor, 'GET', '/kapacitor/v1/service-tests').then(({data: {services}}) => {
    const service = services.find(s => s.name === outputName);
    return kapacitorProxy(kapacitor, 'POST', service.link.href, Object.assign({}, service.options, properties));
  });
}

export function createKapacitorTask(kapacitor, id, type, dbrps, script) {
  return kapacitorProxy(kapacitor, 'POST', '/kapacitor/v1/tasks', {
    id,
    type,
    dbrps,
    script,
    status: 'enabled',
  });
}

export function enableKapacitorTask(kapacitor, id) {
  return kapacitorProxy(kapacitor, 'PATCH', `/kapacitor/v1/tasks/${id}`, {status: 'enabled'});
}

export function disableKapacitorTask(kapacitor, id) {
  return kapacitorProxy(kapacitor, 'PATCH', `/kapacitor/v1/tasks/${id}`, {status: 'disabled'});
}

export function deleteKapacitorTask(kapacitor, id) {
  return kapacitorProxy(kapacitor, 'DELETE', `/kapacitor/v1/tasks/${id}`, '');
}

export function kapacitorProxy(kapacitor, method, path, body) {
  return AJAX({
    method,
    url: kapacitor.links.proxy,
    params: {
      path,
    },
    data: body,
  });
}
