// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {SelectGroup, ButtonShape, Icon, IconFont} from '@influxdata/clockface'

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

type Props = OwnProps & ReduxProps

const DashboardLightModeToggle: FC<Props> = ({theme, onSetTheme}) => {
  return (
    <SelectGroup
      testID="presentation-mode-toggle-group"
      shape={ButtonShape.Square}
    >
      <SelectGroup.Option
        onClick={() => onSetTheme('dark')}
        value={false}
        id="presentation-mode-toggle--dark"
        active={theme === 'dark'}
        titleText="Dark Mode"
      >
        <Icon glyph={IconFont.Moon} />
      </SelectGroup.Option>
      <SelectGroup.Option
        onClick={() => onSetTheme('light')}
        id="presentation-mode-toggle--light"
        value={true}
        active={theme === 'light'}
        titleText="Light Mode"
      >
        <Icon glyph={IconFont.Sun} />
      </SelectGroup.Option>
    </SelectGroup>
  )
}

const mstp = (state: AppState) => {
  const {
    app: {
      persisted: {theme},
    },
  } = state

  return {theme}
}

const mdtp = {
  onSetTheme: setTheme,
}

export default connector(DashboardLightModeToggle)
