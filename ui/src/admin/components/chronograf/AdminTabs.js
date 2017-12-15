import React, {PropTypes} from 'react'

import {
  isUserAuthorized,
  ADMIN_ROLE,
  SUPERADMIN_ROLE,
} from 'src/auth/Authorized'

import {Tab, Tabs, TabPanel, TabPanels, TabList} from 'shared/components/Tabs'
import OrganizationsPage from 'src/admin/containers/OrganizationsPage'
import UsersTable from 'src/admin/components/chronograf/UsersTable'

const ORGANIZATIONS_TAB_NAME = 'Organizations'
const USERS_TAB_NAME = 'Users'

const AdminTabs = ({
  meRole,
  // UsersTable
  users,
  organization,
  onCreateUser,
  onUpdateUserRole,
  onUpdateUserSuperAdmin,
  onDeleteUser,
  meID,
  notify,
}) => {
  const tabs = [
    {
      requiredRole: SUPERADMIN_ROLE,
      type: ORGANIZATIONS_TAB_NAME,
      component: (
        <OrganizationsPage
          currentOrganization={organization}
          onCreateUser={onCreateUser} // allows a SuperAdmin to join organizations where they don't currently have a role
        />
      ),
    },
    {
      requiredRole: ADMIN_ROLE,
      type: USERS_TAB_NAME,
      component: (
        <UsersTable
          users={users}
          organization={organization}
          onCreateUser={onCreateUser}
          onUpdateUserRole={onUpdateUserRole}
          onUpdateUserSuperAdmin={onUpdateUserSuperAdmin}
          onDeleteUser={onDeleteUser}
          meID={meID}
          notify={notify}
        />
      ),
    },
  ].filter(t => isUserAuthorized(meRole, t.requiredRole))

  return (
    <Tabs className="row">
      <TabList customClass="col-md-2 admin-tabs">
        {tabs.map((t, i) =>
          <Tab key={tabs[i].type}>
            {tabs[i].type}
          </Tab>
        )}
      </TabList>
      <TabPanels customClass="col-md-10 admin-tabs--content">
        {tabs.map((t, i) =>
          <TabPanel key={tabs[i].type}>
            {t.component}
          </TabPanel>
        )}
      </TabPanels>
    </Tabs>
  )
}

const {arrayOf, bool, func, shape, string} = PropTypes

AdminTabs.defaultProps = {
  organization: {
    name: '',
    id: '0',
  },
}

AdminTabs.propTypes = {
  meRole: string.isRequired,
  meID: string.isRequired,
  // UsersTable
  users: arrayOf(
    shape({
      id: string,
      links: shape({
        self: string.isRequired,
      }),
      name: string.isRequired,
      provider: string.isRequired,
      roles: arrayOf(
        shape({
          name: string.isRequired,
          organization: string.isRequired,
        })
      ),
      scheme: string.isRequired,
      superAdmin: bool,
    })
  ).isRequired,
  organization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }).isRequired,
  onCreateUser: func.isRequired,
  onUpdateUserRole: func.isRequired,
  onUpdateUserSuperAdmin: func.isRequired,
  onDeleteUser: func.isRequired,
  notify: func.isRequired,
}

export default AdminTabs
