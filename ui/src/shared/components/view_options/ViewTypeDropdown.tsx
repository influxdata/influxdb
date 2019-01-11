// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {setType} from 'src/shared/actions/v2/timeMachines'

// Components
import {Dropdown} from 'src/clockface'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Constants
import {GRAPH_TYPES} from 'src/dashboards/graphics/graph'

// Types
import {View, NewView, AppState, ViewType} from 'src/types/v2'

interface DispatchProps {
  onUpdateType: typeof setType
}

interface StateProps {
  view: View | NewView
}

type Props = DispatchProps & StateProps

class ViewOptions extends PureComponent<Props> {
  public render() {
    return (
      <Dropdown
        selectedID={this.selectedView}
        onChange={this.handleChange}
        widthPixels={154}
      >
        {this.dropdownItems}
      </Dropdown>
    )
  }

  private handleChange = (viewType: ViewType): void => {
    const {onUpdateType} = this.props

    onUpdateType(viewType)
  }

  private get dropdownItems(): JSX.Element[] {
    return GRAPH_TYPES.map(g => (
      <Dropdown.Item
        key={`view-type--${g.type}`}
        id={`${g.type}`}
        value={g.type}
      >
        {g.menuOption}
      </Dropdown.Item>
    ))
  }

  private get selectedView(): string {
    const {view} = this.props

    return `${view.properties.type}`
  }
}

const mstp = (state: AppState): StateProps => {
  const {view} = getActiveTimeMachine(state)

  return {view}
}

const mdtp: DispatchProps = {
  onUpdateType: setType,
}
export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(ViewOptions)
