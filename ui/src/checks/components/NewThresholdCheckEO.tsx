// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {connect, ConnectedProps, useDispatch} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {Overlay, SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import CheckEOHeader from 'src/checks/components/CheckEOHeader'
import TimeMachine from 'src/timeMachine/components/TimeMachine'

// Actions
import {createCheckFromTimeMachine} from 'src/checks/actions/thunks'
import {setActiveTimeMachine} from 'src/timeMachine/actions'
import {
  resetAlertBuilder,
  updateName,
  initializeAlertBuilder,
} from 'src/alerting/actions/alertBuilder'

// Utils
import {createView} from 'src/views/helpers'

// Types
import {AppState, RemoteDataState, CheckViewProperties} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & RouteComponentProps<{orgID: string}>

const NewCheckOverlay: FunctionComponent<Props> = ({
  status,
  match: {
    params: {orgID},
  },
  checkName,
  history,
  onSaveCheckFromTimeMachine,
  onResetAlertBuilder,
  onUpdateAlertBuilderName,
}) => {
  const dispatch = useDispatch()

  useEffect(() => {
    const view = createView<CheckViewProperties>('threshold')
    dispatch(initializeAlertBuilder('threshold'))
    dispatch(
      setActiveTimeMachine('alerting', {
        view,
      })
    )
  }, [dispatch])

  const handleClose = () => {
    history.push(`/orgs/${orgID}/alerting`)
    onResetAlertBuilder()
  }

  return (
    <Overlay visible={true} className="veo-overlay">
      <div className="veo">
        <SpinnerContainer
          spinnerComponent={<TechnoSpinner />}
          loading={status || RemoteDataState.Loading}
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

const mstp = ({alertBuilder: {name, status}}: AppState) => {
  return {checkName: name, status}
}

const mdtp = {
  onSaveCheckFromTimeMachine: createCheckFromTimeMachine as any,
  onResetAlertBuilder: resetAlertBuilder,
  onUpdateAlertBuilderName: updateName,
}

const connector = connect(mstp, mdtp)

export default connector(withRouter(NewCheckOverlay))
