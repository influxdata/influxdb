import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {getOrg} from 'src/organizations/selectors'
import {setTimeZone} from 'src/shared/actions/app'
import {timeZone as timeZoneFromState} from 'src/shared/selectors/app'

import {AppState, TimeZone, Organization} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
export type Props = ReduxProps

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

const mstp = (state: AppState) => {
  const org = getOrg(state)

  return {
    timeZone: timeZoneFromState(state),
    org,
  }
}

const mdtp = {
  onSetTimeZone: setTimeZone,
}

const connector = connect(mstp, mdtp)

export default connector(AppSettingProvider)
