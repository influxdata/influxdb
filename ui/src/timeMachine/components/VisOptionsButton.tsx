// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {SquareButton, IconFont, ComponentColor} from '@influxdata/clockface'

// Actions
import {setActiveTab} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {TimeMachineTab} from 'src/types/timeMachine'
import {AppState} from 'src/types'

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
      activeTab === 'visualization'
        ? ComponentColor.Primary
        : ComponentColor.Default

    return (
      <SquareButton
        color={color}
        icon={IconFont.CogThick}
        onClick={this.handleClick}
      />
    )
  }

  private handleClick = (): void => {
    const {onSetActiveTab, activeTab} = this.props

    if (activeTab !== 'visualization') {
      onSetActiveTab('visualization')
    } else {
      onSetActiveTab('queries')
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
