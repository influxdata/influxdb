// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import VEOHeader from 'src/dashboards/components/VEOHeader'
import TimeMachine from 'src/timeMachine/components/TimeMachine'

// Actions
import {setName} from 'src/timeMachine/actions'
import {saveVEOView} from 'src/dashboards/actions'
import {executeQueries} from 'src/timeMachine/actions/queries'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'
import {Dashboard} from 'src/types'

interface StateProps {
  name: string
  dashboard: Dashboard
}

interface DispatchProps {
  onSetName: typeof setName
  onSaveView: typeof saveVEOView
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
    const {name, onSetName} = this.props

    return (
      <>
        <VEOHeader
          key={name}
          name={name}
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
    const {dashboard, onSaveView, onClose} = this.props

    onSaveView(dashboard)
    onClose()
  }
}

const mstp = (state: AppState, {dashboardID}): StateProps => {
  const {dashboards} = state
  const dashboard = dashboards.list.find(d => d.id === dashboardID)

  const {
    view: {name},
  } = getActiveTimeMachine(state)

  return {name, dashboard}
}

const mdtp: DispatchProps = {
  onSetName: setName,
  onSaveView: saveVEOView,
  executeQueries,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VEOContents)
