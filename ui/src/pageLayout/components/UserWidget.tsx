// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'

// Components
import {TreeNav} from '@influxdata/clockface'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'

// Constants
import {CLOUD_URL, CLOUD_LOGOUT_PATH} from 'src/shared/constants'

// Types
import {AppState, Organization} from 'src/types'
import {MeState} from 'src/shared/reducers/me'

// Selectors
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  org: Organization
  me: MeState
}

const UserWidget: FC<StateProps> = ({org, me}) => {
  const logoutURL = `${CLOUD_URL}${CLOUD_LOGOUT_PATH}`

  if (!org) {
    return null
  }

  return (
    <TreeNav.User username={me.name} team={org.name}>
      <CloudOnly>
        <TreeNav.UserItem
          id="logout"
          label="Logout"
          linkElement={className => (
            <a className={className} href={logoutURL} />
          )}
        />
      </CloudOnly>
      <CloudExclude>
        <TreeNav.UserItem
          id="logout"
          label="Logout"
          linkElement={className => <Link className={className} to="/logout" />}
        />
        <TreeNav.UserItem
          id="switch-orgs"
          label="Switch Organizations"
          linkElement={className => <Link className={className} to="/logout" />}
        />
        <TreeNav.UserItem
          id="create-org"
          label="Create Organization"
          linkElement={className => (
            <Link className={className} to="/orgs/new" />
          )}
        />
      </CloudExclude>
    </TreeNav.User>
  )
}

const mstp = (state: AppState) => {
  const org = getOrg(state)
  const me = state.me
  return {org, me}
}

export default connect<StateProps>(mstp)(UserWidget)
