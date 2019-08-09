// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button} from '@influxdata/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Actions
import {setType as setViewType, addCheck} from 'src/timeMachine/actions'
import {setCurrentCheck} from 'src/alerting/actions/checks'
import {setActiveTab} from 'src/timeMachine/actions'

// Types
import {AppState, RemoteDataState, ViewType, TimeMachineTab} from 'src/types'
import {DEFAULT_THRESHOLD_CHECK} from 'src/alerting/constants'

interface DispatchProps {
  setActiveTab: typeof setActiveTab
  setViewType: typeof setViewType
  setCurrentCheck: typeof setCurrentCheck
  addCheck: typeof addCheck
}

interface StateProps {
  activeTab: TimeMachineTab
  viewType: ViewType
}

type Props = DispatchProps & StateProps

const AlertingButton: FunctionComponent<Props> = ({
  setActiveTab,
  addCheck,
  activeTab,
  setCurrentCheck,
  viewType,
}) => {
  const handleClickAlerting = () => {
    if (viewType !== 'check') {
      setCurrentCheck(RemoteDataState.Done, DEFAULT_THRESHOLD_CHECK)
      addCheck()
    } else {
      setActiveTab('alerting')
    }
  }

  const handleClickQueries = () => {
    setActiveTab('queries')
  }

  if (activeTab === 'alerting') {
    return <Button text="Queries" onClick={handleClickQueries} />
  }

  return (
    <FeatureFlag name="alerting">
      <Button
        titleText="Add alerting to this query"
        text="Alerting"
        onClick={handleClickAlerting}
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
  addCheck: addCheck,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(AlertingButton)
