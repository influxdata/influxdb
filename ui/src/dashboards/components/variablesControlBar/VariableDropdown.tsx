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
import {selectVariableValue} from 'src/dashboards/actions/thunks'

// Utils
import {getVariableValuesForDropdown} from 'src/dashboards/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  values: {name: string; value: string}[]
  selectedValue: string
}

interface DispatchProps {
  onSelectValue: typeof selectVariableValue
}

interface OwnProps {
  variableID: string
  dashboardID: string
  variableName: string
}

type Props = StateProps & DispatchProps & OwnProps

class VariableDropdown extends PureComponent<Props> {
  render() {
    const {selectedValue, variableName} = this.props
    const dropdownValues = this.props.values || []

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
          testID={`variable-dropdown ${variableName}`}
          button={(active, onClick) => (
            <Dropdown.Button
              active={active}
              onClick={onClick}
              testID={`variable-dropdown--button ${variableName}`}
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
            {dropdownValues.map(({name}) => (
                /*
                Use key as value since they are unique otherwise
                multiple selection appear in the dropdown
              */
                <Dropdown.Item
                  key={name}
                  id={name}
                  value={name}
                  onClick={this.handleSelect}
                  selected={name === selectedValue}
                  testID="variable-dropdown--item"
                >
                  {name}
                </Dropdown.Item>
              ))}
            </Dropdown.Menu>
          )}
        />
      </div>
    )
  }

  private handleSelect = (selectedValue: string) => {
    const {dashboardID, variableID, onSelectValue} = this.props

    onSelectValue(dashboardID, variableID, selectedValue)
  }
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  const {dashboardID, variableID} = props

  const {selectedValue, list} = getVariableValuesForDropdown(
    state,
    variableID,
    dashboardID
  )

  return {values: list, selectedValue}
}

const mdtp = {
  onSelectValue: selectVariableValue,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VariableDropdown)
