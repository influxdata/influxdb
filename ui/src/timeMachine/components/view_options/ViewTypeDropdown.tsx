// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {setType} from 'src/timeMachine/actions'

// Components
import {Dropdown, DropdownMenuColors} from 'src/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Constants
import {VIS_GRAPHICS} from 'src/timeMachine/constants/visGraphics'

// Types
import {View, NewView, AppState, ViewType} from 'src/types/v2'

interface DispatchProps {
  onUpdateType: typeof setType
}

interface StateProps {
  view: View | NewView
}

type Props = DispatchProps & StateProps

class ViewTypeDropdown extends PureComponent<Props> {
  public render() {
    return (
      <Dropdown
        selectedID={this.selectedView}
        onChange={this.handleChange}
        widthPixels={215}
        customClass="view-type-dropdown"
        menuColor={DropdownMenuColors.Onyx}
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
    return VIS_GRAPHICS.map(g => (
      <Dropdown.Item
        key={`view-type--${g.type}`}
        id={`${g.type}`}
        value={g.type}
      >
        <div className="view-type-dropdown--graphic">{g.graphic}</div>
        <div className="view-type-dropdown--name">{g.name}</div>
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
)(ViewTypeDropdown)
