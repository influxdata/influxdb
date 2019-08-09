// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, IconFont, ComponentColor} from '@influxdata/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Actions
import {setType as setViewType, addCheckToView} from 'src/timeMachine/actions'
import {setCurrentCheck} from 'src/alerting/actions/checks'
import {setActiveTab} from 'src/timeMachine/actions'

// Types
import {AppState, RemoteDataState, ViewType, TimeMachineTab} from 'src/types'
import {DEFAULT_THRESHOLD_CHECK} from 'src/alerting/constants'

interface DispatchProps {
  setActiveTab: typeof setActiveTab
  setViewType: typeof setViewType
  setCurrentCheck: typeof setCurrentCheck
  addCheckToView: typeof addCheckToView
}

interface StateProps {
  activeTab: TimeMachineTab
  viewType: ViewType
}

type Props = DispatchProps & StateProps

const AlertingButton: FunctionComponent<Props> = ({
  setActiveTab,
  addCheckToView,
  activeTab,
  setCurrentCheck,
  viewType,
}) => {
  const handleClick = () => {
    if (activeTab === 'alerting') {
      setActiveTab('queries')
    } else {
      if (viewType !== 'check') {
        setCurrentCheck(RemoteDataState.Done, DEFAULT_THRESHOLD_CHECK)
        addCheckToView()
      } else {
        setActiveTab('alerting')
      }
    }
  }

  return (
    <FeatureFlag name="alerting">
      <Button
        icon={IconFont.BellSolid}
        color={
          activeTab === 'alerting'
            ? ComponentColor.Secondary
            : ComponentColor.Default
        }
        titleText="Add alerting to this query"
        text="Alerting"
        onClick={handleClick}
      />
    </FeatureFlag>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    activeTab,
    view: {
      properties: {type: viewType},
    },
  } = getActiveTimeMachine(state)

  return {activeTab, viewType}
}

const mdtp: DispatchProps = {
  setActiveTab: setActiveTab,
  setViewType: setViewType,
  setCurrentCheck: setCurrentCheck,
  addCheckToView: addCheckToView,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(AlertingButton)
