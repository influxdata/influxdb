import React, {FC} from 'react'
import {connect} from 'react-redux'
import {setTimeZone} from 'src/shared/actions/app'
import {timeZone as timeZoneFromState} from 'src/shared/selectors/app'
import {getOrg} from 'src/organizations/selectors'
import {getAllVariables} from 'src/variables/selectors'

import {AppState, TimeZone, organization} from 'src/types'

export interface StateProps {
  timeZone: TimeZone
  org: Organization
}

export interface DispatchProps {
  onSetTimeZone: typeof setTimeZone
}

export type Props = StateProps & DispatchProps

type Modifier = typeof setTimeZone
export interface AppSettingContextType {
    org: Organization
  timeZone: TimeZone
  onSetTimeZone: Modifier
}

export const DEFAULT_CONTEXT: AppSettingContextType = {
  timeZone: 'Local' as TimeZone,
  org: '',
  variables: [],
  onSetTimeZone: ((() => {}) as any) as Modifier,
}

export const AppSettingContext = React.createContext<AppSettingContextType>(
  DEFAULT_CONTEXT
)

export const AppSettingProvider: FC<Props> = React.memo(
  ({timeZone, org, variables, onSetTimeZone, children}) => {
    return (
      <AppSettingContext.Provider
        value={{
            org,
            variables,
          timeZone,
          onSetTimeZone,
        }}
      >
        {children}
      </AppSettingContext.Provider>
    )
  }
)

const mstp = (state: AppState): StateProps => {
  return {
      org: getOrg(state),
      variables: getAllVariables(state),
    timeZone: timeZoneFromState(state),
  }
}

const mdtp: DispatchProps = {
  onSetTimeZone: setTimeZone,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(AppSettingProvider)
