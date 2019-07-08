// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {SpinnerContainer, TechnoSpinner, Overlay} from '@influxdata/clockface'
import VEOContents from 'src/dashboards/components/VEOContents'

// Utils
import {getView} from 'src/dashboards/selectors'
import {createView} from 'src/shared/utils/view'

// Types
import {AppState, ViewType, QueryView, XYView, RemoteDataState} from 'src/types'
import {setActiveTimeMachine} from 'src/timeMachine/actions'

interface OwnProps extends WithRouterProps {
  params: {
    dashboardID: string
    cellID?: string
    orgID: string
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

class VEO extends PureComponent<Props> {
  public render() {
    const {params} = this.props

    return (
      <Overlay visible={true} className="veo-overlay">
        <div className="veo">
          <SpinnerContainer
            spinnerComponent={<TechnoSpinner />}
            loading={this.loading}
          >
            <VEOContents
              dashboardID={params.dashboardID}
              onClose={this.handleClose}
            />
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
      params: {dashboardID, orgID},
    } = this.props

    router.push(`/orgs/${orgID}/dashboards/${dashboardID}`)
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
