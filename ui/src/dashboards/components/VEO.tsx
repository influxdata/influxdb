// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

import {Overlay} from 'src/clockface'

// Components
import VEOHeader from 'src/dashboards/components/VEOHeader'
import TimeMachine from 'src/timeMachine/components/TimeMachine'

// Actions
import {setName} from 'src/timeMachine/actions'
import * as viewActions from 'src/dashboards/actions/views'
import * as dashboardActions from 'src/dashboards/actions'
import {notify} from 'src/shared/actions/notifications'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {createView} from 'src/shared/utils/view'

// Types
import {AppState, DashboardQuery, ViewType} from 'src/types/v2'
import {QueryView} from 'src/types/v2/dashboards'
import {Dashboard} from 'src/types'
import {XYView} from 'src/types/v2/dashboards'
import {setActiveTimeMachine} from 'src/timeMachine/actions'

// Constants
import {VEO_TIME_MACHINE_ID} from 'src/timeMachine/constants'

// Nofication Messages
import {cellAddFailed} from 'src/shared/copy/notifications'
import {executeQueries} from 'src/timeMachine/actions/queries'

interface StateProps {
  draftView: QueryView
  existingView?: QueryView
  draftQueries: DashboardQuery[]
  dashboard: Dashboard
}

interface DispatchProps {
  onSetName: typeof setName
  onCreateCellWithView: typeof dashboardActions.createCellWithView
  onUpdateView: typeof viewActions.updateView
  notify: typeof notify
  setActiveTimeMachine: typeof setActiveTimeMachine
  executeQueries: typeof executeQueries
}

interface OwnProps extends WithRouterProps {
  params: {
    dashboardID: string
    cellID?: string
  }
}

type Props = OwnProps & StateProps & DispatchProps

class VEO extends PureComponent<Props, {}> {
  public componentDidMount() {
    const {existingView} = this.props
    const view = existingView || createView<XYView>(ViewType.XY)

    this.props.setActiveTimeMachine(VEO_TIME_MACHINE_ID, {view})

    this.props.executeQueries()
  }

  public render() {
    const {draftView, onSetName} = this.props

    return (
      <Overlay visible={true} className="veo-overlay">
        <div className="veo">
          <VEOHeader
            key={draftView.name}
            name={draftView.name}
            onSetName={onSetName}
            onCancel={this.close}
            onSave={this.handleSave}
          />
          <div className="veo-contents">
            <TimeMachine />
          </div>
        </div>
      </Overlay>
    )
  }

  private handleSave = async (): Promise<void> => {
    const {
      draftView,
      draftQueries,
      dashboard,
      onCreateCellWithView,
      onUpdateView,
      notify,
    } = this.props

    const view: QueryView & {id?: string} = {
      ...draftView,
      properties: {
        ...draftView.properties,
        queries: draftQueries,
      },
    }

    try {
      if (view.id) {
        await onUpdateView(dashboard.id, view)
      } else {
        await onCreateCellWithView(dashboard, view)
      }
      this.close()
    } catch (error) {
      console.error(error)
      notify(cellAddFailed())
    }
  }

  private close = (): void => {
    this.props.router.goBack()
  }
}

const mstp = (state: AppState, {params}): StateProps => {
  const {cellID, dashboardID} = params
  const {
    views: {views},
    dashboards,
  } = state
  const {view, draftQueries} = getActiveTimeMachine(state)
  const dashboard = dashboards.find(d => d.id === dashboardID)
  let existingView = null
  if (cellID) {
    existingView = _.get(views, [cellID, 'view'])
  }

  return {draftView: view, existingView, draftQueries, dashboard}
}

const mdtp: DispatchProps = {
  onSetName: setName,
  onCreateCellWithView: dashboardActions.createCellWithView,
  onUpdateView: viewActions.updateView,
  setActiveTimeMachine,
  notify,
  executeQueries,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<OwnProps, {}>(VEO))
