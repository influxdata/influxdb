// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {setType} from 'src/timeMachine/actions'

// Components
import {Dropdown, DropdownMenuTheme} from '@influxdata/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Constants
import {VIS_GRAPHICS} from 'src/timeMachine/constants/visGraphics'

// Types
import {View, NewView, AppState, ViewType} from 'src/types'

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
        widthPixels={215}
        className="view-type-dropdown"
        button={(active, onClick) => (
          <Dropdown.Button active={active} onClick={onClick}>
            {this.getVewTypeGraphic(this.selectedView)}
          </Dropdown.Button>
        )}
        menu={onCollapse => (
          <Dropdown.Menu onCollapse={onCollapse} theme={DropdownMenuTheme.Onyx}>
            {this.dropdownItems}
          </Dropdown.Menu>
        )}
      />
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
        onClick={this.handleChange}
        selected={`${g.type}` === this.selectedView}
      >
        {this.getVewTypeGraphic(g.type)}
      </Dropdown.Item>
    ))
  }

  private get selectedView(): typeof view.properties.type {
    const {view} = this.props

    if (view.properties.type === 'check') {
      return 'xy'
    }

    return view.properties.type
  }

  private getVewTypeGraphic = (viewType: ViewType): JSX.Element => {
    const {graphic, name} = VIS_GRAPHICS.find(
      graphic => graphic.type === viewType
    )

    return (
      <>
        <div className="view-type-dropdown--graphic">{graphic}</div>
        <div className="view-type-dropdown--name">{name}</div>
      </>
    )
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
