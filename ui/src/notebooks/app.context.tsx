import React, {FC, useState} from 'react'
import {connect} from 'react-redux'
import {setTimeZone} from 'src/shared/actions/app'

import {AppState, TimeZone} from 'src/types'

export interface StateProps {
    timeZone: TimeZone
}

export interface DispatchProps {
    onSetTimeZone: typeof setTimeZone
}

export type Props = StateProps & DispatchProps

export interface AppSettingContextType {
    timeZone: TimeZone
    onSetTimeZone: typeof setTimeZone
}

export const DEFAULT_CONTEXT = {
    timeZone: 'Local',
    onSetTimeZone: () => {}
}

export const AppSettingContext = React.createContext<AppSettingContextType>(
  DEFAULT_CONTEXT
)

const AppSettingProvider: FC<Props> = ({timeZone, onSetTimeZone, children}) => {
  return (
    <AppSettingContext.Provider
      value={{
          timeZone,
          onSetTimeZone
      }}
    >
      {children}
    </AppSettingContext.Provider>
  )
}

const mstp = (state: AppState): StateProps => {
    return {
        timeZone: state.app.persisted.timeZone || 'Local'
    }
}

const mdtp: DispatchProps = {
    onSetTimeZone: setTimeZone
}

export default connect<StateProps, DispatchProps>(
    mstp,
    mdtp
)(AppSettingProvider)
