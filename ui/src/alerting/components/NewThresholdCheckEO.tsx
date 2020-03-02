// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Overlay, SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import CheckEOHeader from 'src/alerting/components/CheckEOHeader'
import TimeMachine from 'src/timeMachine/components/TimeMachine'

// Actions
import {saveCheckFromTimeMachine} from 'src/alerting/actions/checks'
import {setActiveTimeMachine} from 'src/timeMachine/actions'
import {
  resetAlertBuilder,
  updateName,
  initializeAlertBuilder,
} from 'src/alerting/actions/alertBuilder'

// Utils
import {createView} from 'src/shared/utils/view'

// Types
import {AppState, RemoteDataState, CheckViewProperties} from 'src/types'

interface DispatchProps {
  onSetActiveTimeMachine: typeof setActiveTimeMachine
  onSaveCheckFromTimeMachine: typeof saveCheckFromTimeMachine
  onResetAlertBuilder: typeof resetAlertBuilder
  onUpdateAlertBuilderName: typeof updateName
  onInitializeAlertBuilder: typeof initializeAlertBuilder
}

interface StateProps {
  checkName: string
  checkStatus: RemoteDataState
}

type Props = DispatchProps & StateProps & WithRouterProps

const NewCheckOverlay: FunctionComponent<Props> = ({
  checkStatus,
  params: {orgID},
  checkName,
  router,
  onSaveCheckFromTimeMachine,
  onSetActiveTimeMachine,
  onResetAlertBuilder,
  onUpdateAlertBuilderName,
  onInitializeAlertBuilder,
}) => {
  useEffect(() => {
    const view = createView<CheckViewProperties>('threshold')
    onInitializeAlertBuilder('threshold')
    onSetActiveTimeMachine('alerting', {
      view,
    })
  }, [])

  const handleClose = () => {
    router.push(`/orgs/${orgID}/alerting`)
    onResetAlertBuilder()
  }

  return (
    <Overlay visible={true} className="veo-overlay">
      <div className="veo">
        <SpinnerContainer
          spinnerComponent={<TechnoSpinner />}
          loading={checkStatus || RemoteDataState.Loading}
        >
          <CheckEOHeader
            key={checkName}
            name={checkName}
            onSetName={onUpdateAlertBuilderName}
            onCancel={handleClose}
            onSave={onSaveCheckFromTimeMachine}
          />
          <div className="veo-contents">
            <TimeMachine />
          </div>
        </SpinnerContainer>
      </div>
    </Overlay>
  )
}

const mstp = ({alertBuilder: {name, checkStatus}}: AppState): StateProps => {
  return {checkName: name, checkStatus}
}

const mdtp: DispatchProps = {
  onSetActiveTimeMachine: setActiveTimeMachine,
  onSaveCheckFromTimeMachine: saveCheckFromTimeMachine,
  onResetAlertBuilder: resetAlertBuilder,
  onUpdateAlertBuilderName: updateName,
  onInitializeAlertBuilder: initializeAlertBuilder,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(NewCheckOverlay))
