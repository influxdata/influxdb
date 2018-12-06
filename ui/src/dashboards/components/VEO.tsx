// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import VEOHeader from 'src/dashboards/components/VEOHeader'
import TimeMachine from 'src/shared/components/TimeMachine'

// Actions
import {setName} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Types
import {Source, AppState, DashboardQuery} from 'src/types/v2'
import {NewView, View} from 'src/types/v2/dashboards'

// Styles
import './VEO.scss'

interface StateProps {
  draftView: NewView
  draftQueries: DashboardQuery[]
}

interface DispatchProps {
  onSetName: typeof setName
}

interface OwnProps {
  source: Source
  onHide: () => void
  onSave: (v: NewView | View) => Promise<void>
}

type Props = OwnProps & StateProps & DispatchProps

class VEO extends PureComponent<Props, {}> {
  public render() {
    const {draftView, onSetName, onHide} = this.props

    return (
      <div className="veo">
        <VEOHeader
          key={draftView.name}
          name={draftView.name}
          onSetName={onSetName}
          onCancel={onHide}
          onSave={this.handleSave}
        />
        <TimeMachine />
      </div>
    )
  }

  private handleSave = (): void => {
    const {draftView, draftQueries, onSave} = this.props

    // Ensure that the latest queries are saved with the view, even if they
    // haven't been submitted yet
    const view = {
      ...draftView,
      properties: {
        ...draftView.properties,
        queries: draftQueries,
      },
    }

    onSave(view)
  }
}

const mstp = (state: AppState): StateProps => {
  const {view, draftQueries} = getActiveTimeMachine(state)

  return {draftView: view, draftQueries}
}

const mdtp: DispatchProps = {
  onSetName: setName,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VEO)
