// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {Overlay} from 'src/clockface'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import VEOContents from 'src/dashboards/components/VEOContents'

// Utils
import {getView} from 'src/dashboards/selectors'
import {createView} from 'src/shared/utils/view'

// Types
import {AppState, ViewType, QueryView, XYView, RemoteDataState} from 'src/types'
import {setActiveTimeMachine} from 'src/timeMachine/actions'

// Constants
import {VEO_TIME_MACHINE_ID} from 'src/timeMachine/constants'

interface OwnProps extends WithRouterProps {
  params: {
    dashboardID: string
    cellID?: string
  }
}

interface StateProps {
  viewsStatus: RemoteDataState
  view: QueryView
}

interface DispatchProps {
  onSetActiveTimeMachine: typeof setActiveTimeMachine
}

type Props = StateProps & DispatchProps & OwnProps

interface State {
  hasActivatedTimeMachine: boolean
}

class VEO extends PureComponent<Props, State> {
  public state: State = {
    hasActivatedTimeMachine: false,
  }

  public componentDidUpdate() {
    const {view, onSetActiveTimeMachine} = this.props
    const {hasActivatedTimeMachine} = this.state

    const timeMachineShouldActivate =
      !hasActivatedTimeMachine && this.loading === RemoteDataState.Done

    if (timeMachineShouldActivate) {
      onSetActiveTimeMachine(VEO_TIME_MACHINE_ID, {view})
      this.setState({hasActivatedTimeMachine: true})
    }
  }

  public render() {
    const {hasActivatedTimeMachine} = this.state
    const {params} = this.props

    return (
      <Overlay visible={true} className="veo-overlay">
        <div className="veo">
          <SpinnerContainer
            spinnerComponent={<TechnoSpinner />}
            loading={this.loading}
          >
            {hasActivatedTimeMachine && (
              <VEOContents
                dashboardID={params.dashboardID}
                onClose={this.handleClose}
              />
            )}
          </SpinnerContainer>
        </div>
      </Overlay>
    )
  }

  private get loading(): RemoteDataState {
    const {viewsStatus, view} = this.props

    if (viewsStatus === RemoteDataState.Done && view) {
      return RemoteDataState.Done
    } else if (viewsStatus === RemoteDataState.Done) {
      return RemoteDataState.Error
    }

    return viewsStatus
  }

  private handleClose = () => {
    const {
      router,
      params: {dashboardID},
    } = this.props

    router.push(`/dashboards/${dashboardID}`)
  }
}

const mstp = (state: AppState, {params}): StateProps => {
  const {cellID} = params
  const {
    views: {status},
  } = state

  if (cellID) {
    const view = getView(state, cellID) as QueryView
    return {view, viewsStatus: status}
  }

  const view = createView<XYView>(ViewType.XY)
  return {view, viewsStatus: status}
}

const mdtp: DispatchProps = {
  onSetActiveTimeMachine: setActiveTimeMachine,
}

export default connect(
  mstp,
  mdtp
)(withRouter<OwnProps, {}>(VEO))
