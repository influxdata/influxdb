import React from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'

import PageHeader from 'src/shared/components/PageHeader'
import SubSections from 'src/shared/components/SubSections'
import FancyScrollbar from 'shared/components/FancyScrollbar'

import UsersPage from 'src/admin/containers/chronograf/UsersPage'
import AllUsersPage from 'src/admin/containers/chronograf/AllUsersPage'
import OrganizationsPage from 'src/admin/containers/chronograf/OrganizationsPage'
import ProvidersPage from 'src/admin/containers/ProvidersPage'

import {
  isUserAuthorized,
  ADMIN_ROLE,
  SUPERADMIN_ROLE,
} from 'src/auth/Authorized'

const sections = me => [
  {
    url: 'current-organization',
    name: 'Current Org',
    enabled: isUserAuthorized(me.role, ADMIN_ROLE),
    component: (
      <UsersPage meID={me.id} meCurrentOrganization={me.currentOrganization} />
    ),
  },
  {
    url: 'all-users',
    name: 'All Users',
    enabled: isUserAuthorized(me.role, SUPERADMIN_ROLE),
    component: <AllUsersPage meID={me.id} />,
  },
  {
    url: 'all-organizations',
    name: 'All Orgs',
    enabled: isUserAuthorized(me.role, SUPERADMIN_ROLE),
    component: (
      <OrganizationsPage meCurrentOrganization={me.currentOrganization} />
    ),
  },
  {
    url: 'organization-mappings',
    name: 'Org Mappings',
    enabled: isUserAuthorized(me.role, SUPERADMIN_ROLE),
    component: <ProvidersPage />,
  },
]

const AdminChronografPage = ({me, source, params: {tab}}) => (
  <div className="page">
    <PageHeader title="Chronograf Admin" />
    <FancyScrollbar className="page-contents">
      <div className="container-fluid">
        <SubSections
          sections={sections(me)}
          activeSection={tab}
          parentUrl="admin-chronograf"
          sourceID={source.id}
        />
      </div>
    </FancyScrollbar>
  </div>
)

const {shape, string} = PropTypes

AdminChronografPage.propTypes = {
  me: shape({
    id: string.isRequired,
    role: string.isRequired,
    currentOrganization: shape({
      name: string.isRequired,
      id: string.isRequired,
    }),
  }).isRequired,
  params: shape({
    tab: string,
  }).isRequired,
  source: shape({
    id: string.isRequired,
    links: shape({
      users: string.isRequired,
    }),
  }).isRequired,
}

const mapStateToProps = ({auth: {me}}) => ({
  me,
})

export default connect(mapStateToProps, null)(AdminChronografPage)
