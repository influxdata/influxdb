// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import VEOHeader from 'src/dashboards/components/VEOHeader'
import TimeMachine from 'src/shared/components/TimeMachine'

// Actions
import {setName, setActiveTimeMachine} from 'src/shared/actions/v2/timeMachines'

// Utils
import {replaceQuery} from 'src/shared/utils/view'
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Constants
import {VEO_TIME_MACHINE_ID} from 'src/shared/constants/timeMachine'

// Types
import {Source, AppState} from 'src/types/v2'
import {NewView, View} from 'src/types/v2/dashboards'

interface StateProps {
  draftView: NewView
  draftScript: string
}

interface DispatchProps {
  onSetName: typeof setName
  onSetActiveTimeMachine: typeof setActiveTimeMachine
}

interface OwnProps {
  source: Source
  view: NewView | View
  onHide: () => void
  onSave: (v: NewView | View) => Promise<void>
}

type Props = OwnProps & StateProps & DispatchProps

class VEO extends PureComponent<Props, {}> {
  public componentDidMount() {
    const {onSetActiveTimeMachine, view} = this.props
    const draftScript: string = get(view, 'properties.queries.0.text', '')

    onSetActiveTimeMachine(VEO_TIME_MACHINE_ID, {view, draftScript})
  }

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
    const {view, draftView, draftScript, onSave} = this.props

    onSave(replaceQuery({...view, ...draftView}, draftScript))
  }
}

const mstp = (state: AppState): StateProps => {
  const {view, draftScript} = getActiveTimeMachine(state)

  return {draftView: view, draftScript}
}

const mdtp: DispatchProps = {
  onSetName: setName,
  onSetActiveTimeMachine: setActiveTimeMachine,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VEO)
