// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Dropdown,
  DropdownMenuTheme,
  ComponentStatus,
} from '@influxdata/clockface'

// Actions
import {selectValue} from 'src/variables/actions/thunks'

// Utils
import {getVariable, normalizeValues} from 'src/variables/selectors'

// Types
import {AppState, RemoteDataState} from 'src/types'

interface StateProps {
  values: string[]
  selectedValue: string
  status: RemoteDataState
}

interface DispatchProps {
  onSelectValue: typeof selectValue
}

interface OwnProps {
  variableID: string
  testID?: string
  onSelect?: () => void
}

type Props = StateProps & DispatchProps & OwnProps

class VariableDropdown extends PureComponent<Props> {
  render() {
    const {selectedValue, values} = this.props

    const dropdownStatus =
      values.length === 0 ? ComponentStatus.Disabled : ComponentStatus.Default

    const widthLength = Math.max(
      140,
      values.reduce(function(a, b) {
        return a.length > b.length ? a : b
      }, '').length * 8
    )

    return (
      <div className="variable-dropdown">
        {/* TODO: Add variable description to title attribute when it is ready */}
        <Dropdown
          style={{width: `${140}px`}}
          className="variable-dropdown--dropdown"
          testID={this.props.testID || 'variable-dropdown'}
          button={(active, onClick) => (
            <Dropdown.Button
              active={active}
              onClick={onClick}
              testID="variable-dropdown--button"
              status={dropdownStatus}
            >
              {this.selectedText}
            </Dropdown.Button>
          )}
          menu={onCollapse => (
            <Dropdown.Menu
              style={{width: `${widthLength}px`}}
              onCollapse={onCollapse}
              theme={DropdownMenuTheme.Amethyst}
            >
              {values.map(val => {
                return (
                  <Dropdown.Item
                    key={val}
                    id={val}
                    value={val}
                    onClick={this.handleSelect}
                    selected={val === selectedValue}
                    testID="variable-dropdown--item"
                    // wrapText={true}
                  >
                    {val}
                  </Dropdown.Item>
                )
              })}
            </Dropdown.Menu>
          )}
        />
      </div>
    )
  }

  private handleSelect = (selectedValue: string) => {
    const {variableID, onSelectValue, onSelect} = this.props

    onSelectValue(variableID, selectedValue)

    if (onSelect) {
      onSelect()
    }
  }

  private get selectedText() {
    const {selectedValue, status} = this.props
    if (status === RemoteDataState.Loading) {
      return 'Loading'
    }

    if (selectedValue) {
      return selectedValue
    }

    return 'No Values'
  }
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  const {variableID} = props
  const variable = getVariable(state, variableID)
  const selected =
    variable.selected && variable.selected.length ? variable.selected[0] : null

  return {
    status: variable.status,
    values: normalizeValues(variable),
    selectedValue: selected,
  }
}

const mdtp = {
  onSelectValue: selectValue,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VariableDropdown)
