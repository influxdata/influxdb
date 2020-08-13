// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Actions
import {setType} from 'src/timeMachine/actions'

// Components
import {Dropdown, DropdownMenuTheme} from '@influxdata/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// Constants
import {VIS_GRAPHICS} from 'src/timeMachine/constants/visGraphics'

// Types
import {AppState, ViewType} from 'src/types'
import {ComponentStatus} from 'src/clockface'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

class ViewTypeDropdown extends PureComponent<Props> {
  public render() {
    return (
      <Dropdown
        style={{width: '215px'}}
        className="view-type-dropdown"
        testID="view-type--dropdown"
        button={(active, onClick) => (
          <Dropdown.Button
            active={active}
            onClick={onClick}
            status={this.dropdownStatus}
          >
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
    return VIS_GRAPHICS.filter(g => {
      if (g.type === 'mosaic' && !isFlagEnabled('mosaicGraphType')) {
        return false
      }
      return true
    }).map(g => {
      return (
        <Dropdown.Item
          key={`view-type--${g.type}`}
          id={`${g.type}`}
          testID={`view-type--${g.type}`}
          value={g.type}
          onClick={this.handleChange}
          selected={`${g.type}` === this.selectedView}
        >
          {this.getVewTypeGraphic(g.type)}
        </Dropdown.Item>
      )
    })
  }

  private get dropdownStatus(): ComponentStatus {
    const {viewType} = this.props

    if (viewType === 'check') {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Valid
  }

  private get selectedView(): ViewType {
    const {viewType} = this.props

    if (viewType === 'check') {
      return 'xy'
    }

    return viewType
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

export {ViewTypeDropdown}

const mstp = (state: AppState) => {
  const {view} = getActiveTimeMachine(state)

  return {viewType: view.properties.type}
}

const mdtp = {
  onUpdateType: setType,
}

const connector = connect(mstp, mdtp)

export default connector(ViewTypeDropdown)
