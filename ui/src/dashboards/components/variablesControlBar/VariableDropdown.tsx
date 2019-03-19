// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {Dropdown, DropdownMenuColors} from 'src/clockface'

// Actions
import {selectVariableValue} from 'src/dashboards/actions/index'

// Utils
import {getValuesForVariable} from 'src/variables/selectors'

// Types
import {AppState} from 'src/types/v2'

interface StateProps {
  values: string[]
  selectedValue: string
}

interface DispatchProps {
  onSelectValue: (
    contextID: string,
    variableID: string,
    value: string
  ) => Promise<void>
}

interface OwnProps {
  name: string
  variableID: string
  dashboardID: string
}

type Props = StateProps & DispatchProps & OwnProps

class VariableDropdown extends PureComponent<Props> {
  render() {
    const {name, selectedValue} = this.props
    const dropdownValues = this.props.values || []

    return (
      <div className="variable-dropdown">
        {/* TODO: Add variable description to title attribute when it is ready */}
        <div className="variable-dropdown--label">
          <span>{name}</span>
        </div>
        <Dropdown
          selectedID={selectedValue}
          onChange={this.handleSelect}
          widthPixels={140}
          titleText="No Values"
          customClass="variable-dropdown--dropdown"
          menuColor={DropdownMenuColors.Amethyst}
        >
          {dropdownValues.map(v => (
            <Dropdown.Item key={v} id={v} value={v}>
              {v}
            </Dropdown.Item>
          ))}
        </Dropdown>
      </div>
    )
  }

  private handleSelect = (value: string) => {
    const {dashboardID, variableID, onSelectValue} = this.props

    onSelectValue(dashboardID, variableID, value)
  }
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  const {dashboardID, variableID} = props

  const {selectedValue, values} = getValuesForVariable(
    state,
    variableID,
    dashboardID
  )

  return {values, selectedValue}
}

const mdtp = {
  onSelectValue: selectVariableValue as any,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VariableDropdown)
