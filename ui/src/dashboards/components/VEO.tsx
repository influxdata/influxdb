// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import VEOHeader from 'src/dashboards/components/VEOHeader'
import TimeMachine from 'src/timeMachine/components/TimeMachine'

// Actions
import {setName} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState, DashboardQuery, View, NewView} from 'src/types/v2'

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
        <div className="veo-contents">
          <TimeMachine />
        </div>
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
