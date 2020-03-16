// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {SelectGroup} from '@influxdata/clockface'

// Actions
import {setTheme} from 'src/shared/actions/app'

// Types
import {AppState, Theme} from 'src/types'

interface StateProps {
  theme: Theme
}

interface DispatchProps {
  onSetTheme: typeof setTheme
}

interface OwnProps {}

type Props = OwnProps & StateProps & DispatchProps

const DashboardLightModeToggle: FC<Props> = ({theme, onSetTheme}) => {
  return (
    <SelectGroup testID="presentation-mode-toggle">
      <SelectGroup.Option
        onClick={() => onSetTheme('dark')}
        value={false}
        id="presentation-mode-toggle--dark"
        active={theme === 'dark'}
      >
        Dark
      </SelectGroup.Option>
      <SelectGroup.Option
        onClick={() => onSetTheme('light')}
        id="presentation-mode-toggle--light"
        value={true}
        active={theme === 'light'}
      >
        Light
      </SelectGroup.Option>
    </SelectGroup>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    app: {
      persisted: {theme},
    },
  } = state

  return {theme}
}

const mdtp: DispatchProps = {
  onSetTheme: setTheme,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DashboardLightModeToggle)
