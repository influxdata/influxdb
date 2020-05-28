import React, {FC, useState} from 'react'
import {connect} from 'react-redux'
import {setTimeZone} from 'src/shared/actions/app'
import {timeZone as timeZoneFromState} from 'src/shared/selectors/app'

import {AppState, TimeZone} from 'src/types'

export interface StateProps {
  timeZone: TimeZone
}

export interface DispatchProps {
  onSetTimeZone: typeof setTimeZone
}

export type Props = StateProps & DispatchProps

type Modifier = typeof setTimeZone
export interface AppSettingContextType {
  timeZone: TimeZone
  onSetTimeZone: Modifier
  miniMapVisibility: boolean
  onToggleMiniMap: () => void
}

export const DEFAULT_CONTEXT: AppSettingContextType = {
  timeZone: 'Local' as TimeZone,
  onSetTimeZone: ((() => {}) as any) as Modifier,
  miniMapVisibility: true,
  onToggleMiniMap: () => {},
}

export const AppSettingContext = React.createContext<AppSettingContextType>(
  DEFAULT_CONTEXT
)

export const AppSettingProvider: FC<Props> = React.memo(
  ({timeZone, onSetTimeZone, children}) => {
    const [miniMapVisibility, setMiniMapVisibility] = useState<boolean>(false)

    const onToggleMiniMap = (): void => {
      console.log('boop')
      setMiniMapVisibility(!miniMapVisibility)
    }

    return (
      <AppSettingContext.Provider
        value={{
          timeZone,
          onSetTimeZone,
          miniMapVisibility,
          onToggleMiniMap,
        }}
      >
        {children}
      </AppSettingContext.Provider>
    )
  }
)

const mstp = (state: AppState): StateProps => {
  return {
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
