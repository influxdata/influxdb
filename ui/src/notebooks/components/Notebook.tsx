// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import QueryBuilderPanel from 'src/notebooks/components/panels/QueryBuilderPanel'
import RawDataPanel from 'src/notebooks/components/panels/RawDataPanel'
import VisualizationPanel from 'src/notebooks/components/panels/VisualizationPanel'
import {FlexBox, JustifyContent, ComponentSize} from '@influxdata/clockface'
import AddVisualizationButton from 'src/notebooks/components/AddVisualizationButton'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  isViewingRawData: boolean
}

const Notebook: FunctionComponent<StateProps> = ({
  isViewingRawData,
}) => {
  return (
    <div className="notebook">
      <QueryBuilderPanel />
      <RawDataPanel />
      {isViewingRawData && <VisualizationPanel />}
      <FlexBox className="notebook--actions" justifyContent={JustifyContent.Center} stretchToFitWidth={true} margin={ComponentSize.Small}>
        <AddVisualizationButton />
      </FlexBox>
    </div>
  )
}

const mstp = (state: AppState) => {
  const {isViewingRawData} = getActiveTimeMachine(state)

  return {isViewingRawData}
}

export default connect<StateProps>(mstp)(Notebook)
