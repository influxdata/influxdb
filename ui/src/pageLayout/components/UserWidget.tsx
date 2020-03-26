// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'

// Components
import {TreeNav} from '@influxdata/clockface'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Actions
import {showOverlay, dismissOverlay} from 'src/overlays/actions/overlays'

// Constants
import {
  CLOUD_URL,
  CLOUD_LOGOUT_PATH,
  CLOUD_USAGE_PATH,
  CLOUD_BILLING_PATH,
} from 'src/shared/constants'

// Types
import {AppState, Organization} from 'src/types'
import {MeState} from 'src/shared/reducers/me'

// Selectors
import {getOrg} from 'src/organizations/selectors'

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

  const logoutURL = `${CLOUD_URL}${CLOUD_LOGOUT_PATH}`

  const handleSwitchOrganizations = (): void => {
    handleShowOverlay('switch-organizations', {}, handleDismissOverlay)
  }

  const logoutLink = (
    <>
      <FeatureFlag name="regionBasedLoginPage">
        <TreeNav.UserItem
          id="logout"
          label="Logout"
          linkElement={className => <Link className={className} to="/logout" />}
        />
      </FeatureFlag>
      <FeatureFlag name="regionBasedLoginPage" equals={false}>
        <CloudExclude>
          <TreeNav.UserItem
            id="logout"
            label="Logout"
            linkElement={className => (
              <Link className={className} to="/logout" />
            )}
          />
        </CloudExclude>
        <CloudOnly>
          <TreeNav.UserItem
            id="logout"
            label="Logout"
            linkElement={className => (
              <a className={className} href={logoutURL} />
            )}
          />
        </CloudOnly>
      </FeatureFlag>
    </>
  )

  return (
    <TreeNav.User username={me.name} team={org.name}>
      <CloudOnly>
        <TreeNav.UserItem
          id="usage"
          label="Usage"
          linkElement={className => (
            <a className={className} href={`${CLOUD_URL}${CLOUD_USAGE_PATH}`} />
          )}
        />
        <TreeNav.UserItem
          id="billing"
          label="Billing"
          linkElement={className => (
            <a
              className={className}
              href={`${CLOUD_URL}${CLOUD_BILLING_PATH}`}
            />
          )}
        />
      </CloudOnly>
      <CloudExclude>
        <TreeNav.UserItem
          id="switch-orgs"
          label="Switch Organizations"
          onClick={handleSwitchOrganizations}
        />
        <TreeNav.UserItem
          id="create-org"
          label="Create Organization"
          linkElement={className => (
            <Link className={className} to="/orgs/new" />
          )}
        />
      </CloudExclude>
      {logoutLink}
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

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(UserWidget)
