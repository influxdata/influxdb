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
import {createCheckFailed} from 'src/shared/copy/notifications'
import {notify} from 'src/shared/actions/notifications'
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
  saveCheckFromTimeMachine: typeof saveCheckFromTimeMachine
  notify: typeof notify
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
  params: {orgID},
  router,
  checkStatus,
  checkName,
  onSetActiveTimeMachine,
  saveCheckFromTimeMachine,
  notify,
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

  const handleSave = () => {
    try {
      saveCheckFromTimeMachine()
      handleClose()
    } catch (e) {
      console.error(e)
      notify(createCheckFailed(e.message))
    }
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

const mstp = ({alertBuilder: {name, checkStatus}}: AppState): StateProps => {
  return {checkName: name, checkStatus}
}

const mdtp: DispatchProps = {
  onSetActiveTimeMachine: setActiveTimeMachine,
  saveCheckFromTimeMachine: saveCheckFromTimeMachine,
  notify: notify,
  onResetAlertBuilder: resetAlertBuilder,
  onUpdateAlertBuilderName: updateName,
  onInitializeAlertBuilder: initializeAlertBuilder,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(NewCheckOverlay))
