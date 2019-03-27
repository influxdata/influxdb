// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

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

// Types
import {AppState, DashboardQuery} from 'src/types'
import {QueryView} from 'src/types/dashboards'
import {Dashboard} from 'src/types'

// Nofication Messages
import {cellAddFailed} from 'src/shared/copy/notifications'
import {executeQueries} from 'src/timeMachine/actions/queries'

interface StateProps {
  draftView: QueryView
  draftQueries: DashboardQuery[]
  dashboard: Dashboard
}

interface DispatchProps {
  onSetName: typeof setName
  onCreateCellWithView: typeof dashboardActions.createCellWithView
  onUpdateView: typeof viewActions.updateView
  notify: typeof notify
  executeQueries: typeof executeQueries
}

interface OwnProps {
  dashboardID: string
  onClose: () => void
}

type Props = OwnProps & StateProps & DispatchProps

class VEOContents extends PureComponent<Props, {}> {
  public componentDidMount() {
    this.props.executeQueries()
  }

  public render() {
    const {draftView, onSetName} = this.props

    return (
      <>
        <VEOHeader
          key={draftView.name}
          name={draftView.name}
          onSetName={onSetName}
          onCancel={this.props.onClose}
          onSave={this.handleSave}
        />
        <div className="veo-contents">
          <TimeMachine />
        </div>
      </>
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
      onClose,
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
      onClose()
    } catch (error) {
      console.error(error)
      notify(cellAddFailed())
    }
  }
}

const mstp = (state: AppState, {dashboardID}): StateProps => {
  const {dashboards} = state
  const dashboard = dashboards.find(d => d.id === dashboardID)

  const {view, draftQueries} = getActiveTimeMachine(state)

  return {draftView: view, draftQueries, dashboard}
}

const mdtp: DispatchProps = {
  onSetName: setName,
  onCreateCellWithView: dashboardActions.createCellWithView,
  onUpdateView: viewActions.updateView,
  notify,
  executeQueries,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VEOContents)
