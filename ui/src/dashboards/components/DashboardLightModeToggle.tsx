// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {SelectGroup} from '@influxdata/clockface'

// Actions
import {
  enableDashboardLightMode,
  disableDashboardLightMode,
} from 'src/shared/actions/app'

// Types
import {AppState} from 'src/types'

interface StateProps {
  dashboardLightMode: boolean
}

interface DispatchProps {
  handleEnableLightMode: typeof enableDashboardLightMode
  handleDisableLightMode: typeof disableDashboardLightMode
}

interface OwnProps {}

type Props = OwnProps & StateProps & DispatchProps

const DashboardLightModeToggle: FC<Props> = ({
  handleEnableLightMode,
  handleDisableLightMode,
  dashboardLightMode,
}) => {
  const handleOptionClick = (mode: boolean): void => {
    if (mode) {
      handleEnableLightMode()
    } else {
      handleDisableLightMode()
    }
  }

  return (
    <SelectGroup testID="presentation-mode-toggle">
      <SelectGroup.Option
        onClick={handleOptionClick}
        id="presentation-mode-toggle--light"
        value={false}
        active={dashboardLightMode === false}
      >
        Dark
      </SelectGroup.Option>
      <SelectGroup.Option
        onClick={handleOptionClick}
        id="presentation-mode-toggle--light"
        value={true}
        active={dashboardLightMode === true}
      >
        Light
      </SelectGroup.Option>
    </SelectGroup>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    app: {
      persisted: {dashboardLightMode},
    },
  } = state

  return {dashboardLightMode}
}

const mdtp: DispatchProps = {
  handleEnableLightMode: enableDashboardLightMode,
  handleDisableLightMode: disableDashboardLightMode,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DashboardLightModeToggle)
