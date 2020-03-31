// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Form,
  DropdownMenuTheme,
  Dropdown,
  ComponentStatus,
} from '@influxdata/clockface'

// Actions
import {
  addVariableToTimeMachine,
  selectVariableValue,
} from 'src/timeMachine/actions/queries'

// Utils
import {getTimeMachineValuesStatus} from 'src/variables/selectors'
import {getVariableValuesForDropdown} from 'src/dashboards/selectors'
import {toComponentStatus} from 'src/shared/utils/toComponentStatus'

// Types
import {RemoteDataState, Variable} from 'src/types'
import {AppState} from 'src/types'

interface StateProps {
  values: {name: string; value: string}[]
  selectedValue: string
  valuesStatus: RemoteDataState
}

interface DispatchProps {
  onAddVariableToTimeMachine: typeof addVariableToTimeMachine
  onSelectVariableValue: typeof selectVariableValue
}

interface OwnProps {
  variable?: Variable
  variableID: string
}

type Props = StateProps & DispatchProps & OwnProps

const VariableTooltipContents: FunctionComponent<Props> = ({
  variableID,
  values,
  selectedValue,
  valuesStatus,
  onAddVariableToTimeMachine,
  onSelectVariableValue,
}) => {
  const dropdownItems = values || []

  const handleMouseEnter = () => {
    if (values || valuesStatus === RemoteDataState.Loading) {
      return
    }

    onAddVariableToTimeMachine(variableID)
  }

  let status = toComponentStatus(valuesStatus)

  if (selectedValue === 'Failed to Load') {
    status = ComponentStatus.Disabled
  }

  const handleSelect = (selectedValue: string) => {
    onSelectVariableValue(variableID, selectedValue)
  }

  return (
    <div onMouseEnter={handleMouseEnter} className="flux-toolbar--popover" data-testid="flux-toolbar--variable-popover">
      <Form.Element label="Value">
        <Dropdown
          style={{width: '200px'}}
          testID="variable--tooltip-dropdown"
          button={(active, onClick) => (
            <Dropdown.Button
              active={active}
              onClick={onClick}
              testID="variable-dropdown--button"
              status={status}
            >
              {selectedValue || 'No Values'}
            </Dropdown.Button>
          )}
          menu={onCollapse => (
            <Dropdown.Menu
              onCollapse={onCollapse}
              theme={DropdownMenuTheme.Amethyst}
            >
              {dropdownItems.map(({name}) => (
                /*
                Use key as value since they are unique otherwise
                multiple selection appear in the dropdown
              */
                <Dropdown.Item
                  key={name}
                  id={name}
                  value={name}
                  onClick={handleSelect}
                  selected={name === selectedValue}
                  testID="variable-dropdown--item"
                >
                  {name}
                </Dropdown.Item>
              ))}
            </Dropdown.Menu>
          )}
        />
      </Form.Element>
    </div>
  )
}

const mstp = (state: AppState, ownProps: OwnProps) => {
  const valuesStatus = getTimeMachineValuesStatus(state)
  const {variableID} = ownProps
  const activeTimeMachineID = state.timeMachines.activeTimeMachineID
  const {list, selectedValue} = getVariableValuesForDropdown(
    state,
    variableID,
    activeTimeMachineID
  )
  return {values: list, selectedValue, valuesStatus}
}

const mdtp = {
  onAddVariableToTimeMachine: addVariableToTimeMachine,
  onSelectVariableValue: selectVariableValue,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VariableTooltipContents)
