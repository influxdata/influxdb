// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import VEOHeader from 'src/dashboards/components/VEOHeader'
import TimeMachine from 'src/shared/components/TimeMachine'

// Actions
import {setName} from 'src/shared/actions/v2/timeMachines'

// Utils
import {replaceQuery} from 'src/shared/utils/view'
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Types
import {Source, AppState} from 'src/types/v2'
import {NewView, View} from 'src/types/v2/dashboards'

interface StateProps {
  draftView: NewView
  draftScript: string
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
    const {draftView, draftScript, onSave} = this.props

    // Ensure that the latest script is saved with the view, even if it hasn't
    // been submitted yet
    const view = replaceQuery(draftView, draftScript)

    onSave(view)
  }
}

const mstp = (state: AppState): StateProps => {
  const {view, draftScript} = getActiveTimeMachine(state)

  return {draftView: view, draftScript}
}

const mdtp: DispatchProps = {
  onSetName: setName,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VEO)
