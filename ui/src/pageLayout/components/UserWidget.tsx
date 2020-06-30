// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import {TreeNav} from '@influxdata/clockface'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'

// Actions
import {showOverlay, dismissOverlay} from 'src/overlays/actions/overlays'

// Constants
import {
  CLOUD_URL,
  CLOUD_USAGE_PATH,
  CLOUD_BILLING_PATH,
  CLOUD_USERS_PATH,
} from 'src/shared/constants'

// Types
import {AppState, Organization} from 'src/types'
import {MeState} from 'src/shared/reducers/me'

// Selectors
import {getOrg} from 'src/organizations/selectors'
import {getNavItemActivation} from '../utils'

interface StateProps {
  org: Organization
  me: MeState
}

interface DispatchProps {
  handleShowOverlay: typeof showOverlay
  handleDismissOverlay: typeof dismissOverlay
}

type Props = StateProps & DispatchProps

const UserWidget: FC<Props> = ({
  org,
  me,
  handleShowOverlay,
  handleDismissOverlay,
}) => {
  if (!org) {
    return null
  }

  const handleSwitchOrganizations = (): void => {
    handleShowOverlay('switch-organizations', {}, handleDismissOverlay)
  }

  const orgPrefix = `/orgs/${org.id}`

  return (
    <TreeNav.User username={me.name} team={org.name} testID="user-nav">
      <CloudOnly>
        <TreeNav.UserItem
          id="usage"
          label="Usage"
          testID="user-nav-item-usage"
          linkElement={className => (
            <a className={className} href={`${CLOUD_URL}${CLOUD_USAGE_PATH}`} />
          )}
        />
        <TreeNav.UserItem
          id="billing"
          label="Billing"
          testID="user-nav-item-billing"
          linkElement={className => (
            <a
              className={className}
              href={`${CLOUD_URL}${CLOUD_BILLING_PATH}`}
            />
          )}
        />
        <TreeNav.UserItem
          id="users"
          label="Users"
          testID="user-nav-item-users"
          linkElement={className => (
            <a
              className={className}
              href={`${CLOUD_URL}/organizations/${org.id}${CLOUD_USERS_PATH}`}
            />
          )}
        />
        <TreeNav.UserItem
          id="about"
          label="About"
          testID="user-nav-item-about"
          linkElement={className => (
            <Link className={className} to={`${orgPrefix}/about`} />
          )}
        />
      </CloudOnly>
      <CloudExclude>
        <TreeNav.UserItem
          id="members"
          label="Members"
          testID="user-nav-item-members"
          active={getNavItemActivation(['members'], location.pathname)}
          linkElement={className => (
            <Link className={className} to={`${orgPrefix}/members`} />
          )}
        />
        <TreeNav.UserItem
          id="about"
          label="About"
          testID="user-nav-item-about"
          active={getNavItemActivation(['about'], location.pathname)}
          linkElement={className => (
            <Link className={className} to={`${orgPrefix}/about`} />
          )}
        />
        <TreeNav.UserItem
          id="switch-orgs"
          label="Switch Organizations"
          testID="user-nav-item-switch-orgs"
          onClick={handleSwitchOrganizations}
        />
        <TreeNav.UserItem
          id="create-org"
          label="Create Organization"
          testID="user-nav-item-create-orgs"
          linkElement={className => (
            <Link className={className} to="/orgs/new" />
          )}
        />
      </CloudExclude>
      <TreeNav.UserItem
        id="logout"
        label="Logout"
        testID="user-nav-item-logout"
        linkElement={className => <Link className={className} to="/logout" />}
      />
    </TreeNav.User>
  )
}

const mstp = (state: AppState) => {
  const org = getOrg(state)
  const me = state.me
  return {org, me}
}

const mdtp = {
  handleShowOverlay: showOverlay,
  handleDismissOverlay: dismissOverlay,
}

export default connect<StateProps, DispatchProps>(mstp, mdtp)(UserWidget)
