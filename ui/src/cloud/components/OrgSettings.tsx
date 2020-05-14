// Libraries
import {FunctionComponent, useEffect, useState} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Constants
import {CLOUD} from 'src/shared/constants'

// Types
import {AppState, Organization} from 'src/types'

// Actions
import {getOrgSettings as getOrgSettingsAction} from 'src/cloud/actions/orgsettings'

interface PassedInProps {
  children: React.ReactElement<any>
}
interface StateProps {
  org: Organization
}
interface DispatchProps {
  getOrgSettings: typeof getOrgSettingsAction
}

type Props = StateProps & DispatchProps & PassedInProps

const OrgSettings: FunctionComponent<Props> = ({
  org,
  getOrgSettings,
  children,
}) => {
  const [hasFetchedOrgSettings, setHasFetchedOrgSettings] = useState<boolean>(
    false
  )
  useEffect(() => {
    if (CLOUD && org && !hasFetchedOrgSettings) {
      setHasFetchedOrgSettings(true)
      getOrgSettings()
    }
  }, [org])

  return children
}

const mstp = (state: AppState): StateProps => ({
  org: get(state, 'resources.orgs.org', null),
})

const mdtp: DispatchProps = {
  getOrgSettings: getOrgSettingsAction,
}

export default connect<StateProps, DispatchProps, PassedInProps>(
  mstp,
  mdtp
)(OrgSettings)
