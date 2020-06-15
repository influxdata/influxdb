import React, {FC} from 'react'
import {connect} from 'react-redux'
import {getOrg} from 'src/organizations/selectors'
import {setTimeZone} from 'src/shared/actions/app'
import {timeZone as timeZoneFromState} from 'src/shared/selectors/app'

import {AppState, TimeZone, Organization} from 'src/types'

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
  org: null,
  timeZone: 'Local' as TimeZone,
  onSetTimeZone: ((() => {}) as any) as Modifier,
}

export const AppSettingContext = React.createContext<AppSettingContextType>(
  DEFAULT_CONTEXT
)

export const AppSettingProvider: FC<Props> = React.memo(
  ({org, timeZone, onSetTimeZone, children}) => {
    return (
      <AppSettingContext.Provider
        value={{
          org,
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
  const org = getOrg(state)

  return {
    timeZone: timeZoneFromState(state),
    org,
  }
}

const mdtp: DispatchProps = {
  onSetTimeZone: setTimeZone,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(AppSettingProvider)
