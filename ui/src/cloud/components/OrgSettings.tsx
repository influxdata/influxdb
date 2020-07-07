// Libraries
import {FunctionComponent, useEffect, useState} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Constants
import {CLOUD} from 'src/shared/constants'

// Types
import {AppState, Organization, OrgSetting} from 'src/types'

// Actions
import {getOrgSettings as getOrgSettingsAction} from 'src/cloud/actions/orgsettings'

import {getOrg} from 'src/organizations/selectors'
import {getOrgSettings} from 'src/cloud/selectors/orgsettings'
import {updateReportingContext} from 'src/cloud/utils/reporting'

interface PassedInProps {
  children: React.ReactElement<any>
}
interface StateProps {
  org: Organization
  settings: OrgSetting[]
}
interface DispatchProps {
  getOrgSettings: typeof getOrgSettingsAction
}

type Props = ReduxProps & PassedInProps

const OrgSettings: FunctionComponent<Props> = ({
  org,
  getOrgSettings,
  settings,
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

  useEffect(() => {
    updateReportingContext(
      settings.reduce((prev, curr) => {
        prev[`org (${curr.key})`] = curr.value
        return prev
      }, {})
    )
  }, [settings])

  return children
}

const mstp = (state: AppState) => ({
  org: getOrg(state),
  settings: getOrgSettings(state),
})

const mdtp = {
  getOrgSettings: getOrgSettingsAction,
}

export default connector(OrgSettings)
