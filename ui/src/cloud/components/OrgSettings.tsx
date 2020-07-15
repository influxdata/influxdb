// Libraries
import {FC, useEffect, useState} from 'react'
import {connect, ConnectedProps, useDispatch} from 'react-redux'

// Constants
import {CLOUD} from 'src/shared/constants'

// Types
import {AppState} from 'src/types'

// Actions
import {getOrgSettings as getOrgSettingsAction} from 'src/cloud/actions/orgsettings'

import {getOrg} from 'src/organizations/selectors'
import {getOrgSettings} from 'src/cloud/selectors/orgsettings'
import {updateReportingContext} from 'src/cloud/utils/reporting'

interface PassedInProps {
  children: React.ReactElement<any>
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & PassedInProps

const OrgSettings: FC<Props> = ({org, settings, children}) => {
  const dispatch = useDispatch()
  const [hasFetchedOrgSettings, setHasFetchedOrgSettings] = useState<boolean>(
    false
  )

  useEffect(() => {
    if (CLOUD && org && !hasFetchedOrgSettings) {
      setHasFetchedOrgSettings(true)
      dispatch(getOrgSettingsAction())
    }
  }, [dispatch, org, hasFetchedOrgSettings])

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

const connector = connect(mstp, mdtp)

export default connector(OrgSettings)
