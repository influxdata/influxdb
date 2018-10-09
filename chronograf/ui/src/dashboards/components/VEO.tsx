// Libraries
import React, {PureComponent, ComponentClass} from 'react'
import {connect} from 'react-redux'

// Components
import VEOHeader from 'src/dashboards/components/VEOHeader'

// Actions
import {setViewName} from 'src/dashboards/actions/v2/veo'

// Types
import {Source, AppState} from 'src/types/v2'
import {View} from 'src/types/v2/dashboards'
import {TimeMachineTab} from 'src/types/v2/timeMachine'

interface StateProps {
  viewName: string
}

interface DispatchProps {
  onSetViewName: typeof setViewName
}

interface PassedProps {
  source: Source
  view: View
  onHide: () => void
  onSave: (v: View) => Promise<void>
}

type Props = PassedProps & StateProps & DispatchProps

interface State {
  activeTab: TimeMachineTab
}

class VEO extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      activeTab: TimeMachineTab.Queries,
    }
  }

  public componentDidMount() {
    const {onSetViewName, view} = this.props
    const name = view.name || ''

    // TODO: Collect other view attributes here and set the draft VEO state in
    // Redux with them. Make this a single action
    onSetViewName(name)
  }

  public render() {
    const {viewName, onSetViewName, onHide} = this.props
    const {activeTab} = this.state

    return (
      <div className="veo">
        <VEOHeader
          key={viewName}
          viewName={viewName}
          onSetViewName={onSetViewName}
          activeTab={activeTab}
          onSetActiveTab={this.handleSetActiveTab}
          onCancel={onHide}
          onSave={this.handleSave}
        />
        <div className="veo--body" />
      </div>
    )
  }

  private handleSetActiveTab = (activeTab: TimeMachineTab): void => {
    this.setState({activeTab})
  }

  private handleSave = (): void => {
    const {view, viewName, onSave} = this.props

    // TODO: Collect draft view state from Redux here
    const newView = {...view, name: viewName}

    onSave(newView)
  }
}

const mstp = (state: AppState): StateProps => {
  const {viewName} = state.veo

  return {viewName}
}

const mdtp: DispatchProps = {
  onSetViewName: setViewName,
}

export default connect(mstp, mdtp)(VEO) as ComponentClass<PassedProps, State>
