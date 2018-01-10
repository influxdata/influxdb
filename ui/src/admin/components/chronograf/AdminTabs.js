import React, {PropTypes} from 'react'

import {
  isUserAuthorized,
  ADMIN_ROLE,
  SUPERADMIN_ROLE,
} from 'src/auth/Authorized'

import {Tab, Tabs, TabPanel, TabPanels, TabList} from 'shared/components/Tabs'
import OrganizationsPage from 'src/admin/containers/chronograf/OrganizationsPage'
import UsersPage from 'src/admin/containers/chronograf/UsersPage'
import AllUsersPage from 'src/admin/containers/chronograf/AllUsersPage'

const ORGANIZATIONS_TAB_NAME = 'Organizations'
const CURRENT_ORG_USERS_TAB_NAME = 'Current Org Users'
const ALL_USERS_TAB_NAME = 'All Users'

const AdminTabs = ({
  me: {currentOrganization: meCurrentOrganization, role: meRole, id: meID},
}) => {
  const tabs = [
    {
      requiredRole: SUPERADMIN_ROLE,
      type: ORGANIZATIONS_TAB_NAME,
      component: (
        <OrganizationsPage meCurrentOrganization={meCurrentOrganization} />
      ),
    },
    {
      requiredRole: ADMIN_ROLE,
      type: CURRENT_ORG_USERS_TAB_NAME,
      component: (
        <UsersPage meID={meID} meCurrentOrganization={meCurrentOrganization} />
      ),
    },
    {
      requiredRole: SUPERADMIN_ROLE,
      type: ALL_USERS_TAB_NAME,
      component: (
        <AllUsersPage
          meID={meID}
          meCurrentOrganization={meCurrentOrganization}
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

const {shape, string} = PropTypes

AdminTabs.propTypes = {
  me: shape({
    id: string.isRequired,
    role: string.isRequired,
    currentOrganization: shape({
      name: string.isRequired,
      id: string.isRequired,
    }),
  }).isRequired,
}

export default AdminTabs
