// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, ComponentColor, IconFont, ComponentStatus} from '@influxdata/clockface'

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

const AddVisualizationButton: FC<Props> = ({isViewingRawData, onSetIsViewingRawData}) => {
  const handleToggleIsViewingRawData = (): void => {
    onSetIsViewingRawData(true)
  }

  const buttonStatus = isViewingRawData ? ComponentStatus.Disabled : ComponentStatus.Default

  return (
    <Button text="Add Visualization" icon={IconFont.BarChart} status={buttonStatus} color={ComponentColor.Primary} onClick={handleToggleIsViewingRawData} />
  )
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
)(AddVisualizationButton)
