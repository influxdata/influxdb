// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Overlay, SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import CheckEOHeader from 'src/alerting/components/CheckEOHeader'
import TimeMachine from 'src/timeMachine/components/TimeMachine'

// Actions
import {
  updateCheck,
  setCurrentCheck,
  updateCurrentCheck,
  saveCurrentCheck,
} from 'src/alerting/actions/checks'
import {setActiveTimeMachine} from 'src/timeMachine/actions'

// Utils
import {createView} from 'src/shared/utils/view'

// Types
import {Check, AppState, RemoteDataState, CheckViewProperties} from 'src/types'
import {DEFAULT_THRESHOLD_CHECK} from 'src/alerting/constants'

interface DispatchProps {
  updateCheck: typeof updateCheck
  setCurrentCheck: typeof setCurrentCheck
  updateCurrentCheck: typeof updateCurrentCheck
  onSetActiveTimeMachine: typeof setActiveTimeMachine
  saveCurrentCheck: typeof saveCurrentCheck
}

interface StateProps {
  check: Partial<Check>
  status: RemoteDataState
}

type Props = DispatchProps & StateProps & WithRouterProps

const NewCheckOverlay: FunctionComponent<Props> = ({
  onSetActiveTimeMachine,
  updateCurrentCheck,
  setCurrentCheck,
  saveCurrentCheck,
  params,
  router,
  status,
  check,
}) => {
  useEffect(() => {
    setCurrentCheck(RemoteDataState.Done, DEFAULT_THRESHOLD_CHECK)
    const view = createView<CheckViewProperties>('check')
    onSetActiveTimeMachine('alerting', {
      view,
      activeTab: 'queries',
    })
  }, [])

  const handleUpdateName = (name: string) => {
    updateCurrentCheck({name})
  }

  const handleClose = () => {
    setCurrentCheck(RemoteDataState.NotStarted, null)
    router.push(`/orgs/${params.orgID}/alerting`)
  }

  const handleSave = () => {
    // todo: when check has own view
    // save view as view
    // put view.id on check.viewID
    saveCurrentCheck()
    handleClose()
  }

  return (
    <Overlay visible={true} className="veo-overlay">
      <div className="veo">
        <SpinnerContainer
          spinnerComponent={<TechnoSpinner />}
          loading={status || RemoteDataState.Loading}
        >
          <CheckEOHeader
            key={check && check.name}
            name={check && check.name}
            onSetName={handleUpdateName}
            onCancel={handleClose}
            onSave={handleSave}
          />
          <div className="veo-contents">
            <TimeMachine />
          </div>
        </SpinnerContainer>
      </div>
    </Overlay>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    checks: {
      current: {check, status},
    },
  } = state

  return {check, status}
}

const mdtp: DispatchProps = {
  updateCheck: updateCheck,
  setCurrentCheck: setCurrentCheck,
  updateCurrentCheck: updateCurrentCheck,
  onSetActiveTimeMachine: setActiveTimeMachine,
  saveCurrentCheck: saveCurrentCheck,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(NewCheckOverlay))
