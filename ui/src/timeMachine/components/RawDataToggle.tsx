// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {SlideToggle, InputLabel, ComponentSize} from '@influxdata/clockface'

// Actions
import {setIsViewingRawData} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  isViewingRawData: boolean
}

interface DispatchProps {
  onSetIsViewingRawData: typeof setIsViewingRawData
}

type Props = StateProps & DispatchProps

class TimeMachineQueries extends PureComponent<Props> {
  public render() {
    const {isViewingRawData} = this.props

    return (
      <div className="view-raw-data-toggle">
        <InputLabel>View Raw Data</InputLabel>
        <SlideToggle
          active={isViewingRawData}
          onChange={this.handleToggleIsViewingRawData}
          size={ComponentSize.ExtraSmall}
          testID="raw-data--toggle"
        />
      </div>
    )
  }

  private handleToggleIsViewingRawData = () => {
    const {isViewingRawData, onSetIsViewingRawData} = this.props

    onSetIsViewingRawData(!isViewingRawData)
  }
}

const mstp = (state: AppState) => {
  const {isViewingRawData} = getActiveTimeMachine(state)

  return {isViewingRawData}
}

const mdtp = {
  onSetIsViewingRawData: setIsViewingRawData,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeMachineQueries)
