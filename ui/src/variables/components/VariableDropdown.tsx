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
import {selectValue} from 'src/variables/actions/creators'

// Utils
import {getVariable} from 'src/variables/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
    type: string
  values: {name: string; value: string}[]
  selectedValue: string
}

interface DispatchProps {
  onSelectValue: typeof selectValue
}

interface OwnProps {
  variableID: string
  contextID: string
  onSelect?: () => void
}

type Props = StateProps & DispatchProps & OwnProps

class VariableDropdown extends PureComponent<Props> {
  render() {
    const {selectedValue, type} = this.props
    const dropdownValues = (type === 'map' ? Object.keys(this.props.values) : this.props.values) || []

    const dropdownStatus =
      dropdownValues.length === 0
        ? ComponentStatus.Disabled
        : ComponentStatus.Default

    return (
      <div className="variable-dropdown">
        {/* TODO: Add variable description to title attribute when it is ready */}
        <Dropdown
          style={{width: `${140}px`}}
          className="variable-dropdown--dropdown"
          testID={ this.props.testID || "variable-dropdown" }
          button={(active, onClick) => (
            <Dropdown.Button
              active={active}
              onClick={onClick}
              testID="variable-dropdown--button"
              status={dropdownStatus}
            >
              {selectedValue || 'No Values'}
            </Dropdown.Button>
          )}
          menu={onCollapse => (
            <Dropdown.Menu
              onCollapse={onCollapse}
              theme={DropdownMenuTheme.Amethyst}
            >
              {dropdownValues.map((val) => {
                      return (
                        <Dropdown.Item
                          key={val}
                          id={val}
                          value={val}
                          onClick={this.handleSelect}
                          selected={val === selectedValue}
                          testID="variable-dropdown--item"
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
    const {contextID, variableID, onSelectValue, onSelect} = this.props

    onSelectValue(contextID, variableID, selectedValue)

    if (onSelect) {
        onSelect()
    }
  }
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  const {contextID, variableID} = props

  const variable = getVariable(
      state,
      contextID,
      variableID
  )

  return {
      type: variable.arguments.type,
      values: variable.arguments.values,
      selectedValue: variable.selected
  }
}

const mdtp = {
  onSelectValue: selectValue,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VariableDropdown)
