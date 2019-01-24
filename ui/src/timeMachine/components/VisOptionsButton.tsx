// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, ButtonShape, IconFont, ComponentColor} from 'src/clockface'

// Actions
import {setActiveTab} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'
import {AppState} from 'src/types/v2'

interface StateProps {
  activeTab: TimeMachineTab
}

interface DispatchProps {
  onSetActiveTab: typeof setActiveTab
}

type Props = StateProps & DispatchProps

class VisOptionsButton extends Component<Props> {
  public render() {
    const {activeTab} = this.props

    const color =
      activeTab === TimeMachineTab.Visualization
        ? ComponentColor.Primary
        : ComponentColor.Default

    return (
      <Button
        color={color}
        shape={ButtonShape.Square}
        icon={IconFont.CogThick}
        onClick={this.handleClick}
      />
    )
  }

  private handleClick = (): void => {
    const {onSetActiveTab, activeTab} = this.props

    if (activeTab === TimeMachineTab.Queries) {
      onSetActiveTab(TimeMachineTab.Visualization)
    } else {
      onSetActiveTab(TimeMachineTab.Queries)
    }
  }
}

const mstp = (state: AppState): StateProps => {
  const {activeTab} = getActiveTimeMachine(state)

  return {activeTab}
}

const mdtp: DispatchProps = {
  onSetActiveTab: setActiveTab,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(VisOptionsButton)
