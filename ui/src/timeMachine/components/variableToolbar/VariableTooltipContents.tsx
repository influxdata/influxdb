// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {
  Form,
  SelectDropdown,
  IconFont,
  Dropdown,
  ComponentStatus,
} from '@influxdata/clockface'

// Actions
import {
  addVariableToTimeMachine,
  selectVariableValue,
} from 'src/timeMachine/actions/queries'

// Utils
import {
  getTimeMachineValues,
  getTimeMachineValuesStatus,
} from 'src/variables/selectors'
import {toComponentStatus} from 'src/shared/utils/toComponentStatus'

// Types
import {RemoteDataState, Variable, VariableValues} from 'src/types'
import {AppState} from 'src/types'

interface StateProps {
  values?: VariableValues
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
  valuesStatus,
  onAddVariableToTimeMachine,
  onSelectVariableValue,
}) => {
  let dropdownItems = get(values, 'values', []) || []

  if (Object.keys(dropdownItems).length > 0) {
    dropdownItems = Object.keys(dropdownItems)
  }

  const handleMouseEnter = () => {
    if (values || valuesStatus === RemoteDataState.Loading) {
      return
    }

    onAddVariableToTimeMachine(variableID)
  }

  let selectedOption = 'None Selected'
  let icon
  let status = toComponentStatus(valuesStatus)
  // this should set the selectedKey as the key
  // set as a ternary in order to get e2e test to pass since values are undefined on the test
  const key = values && values.selectedKey ? values.selectedKey : undefined

  if (!values) {
    selectedOption = 'Failed to Load'
    icon = IconFont.AlertTriangle
    status = ComponentStatus.Disabled
  } else if (values.error) {
    selectedOption = 'Failed to Load'
    icon = IconFont.AlertTriangle
    status = ComponentStatus.Disabled
  } else if (key === undefined || values.values[key] === undefined) {
    selectedOption = 'No Results'
  } else {
    selectedOption = get(values, 'selectedKey', 'None Selected')
  }

  const button = () => (
    <Dropdown.Button status={ComponentStatus.Disabled} onClick={() => {}}>
      {selectedOption}
    </Dropdown.Button>
  )

  const menu = () => null

  return (
    <div onMouseEnter={handleMouseEnter}>
      <Form.Element label="Value">
        {dropdownItems.length === 1 ? (
          <Dropdown
            button={button}
            style={{width: '200px'}}
            menu={menu}
            testID="variable--tooltip-dropdown"
          />
        ) : (
          <SelectDropdown
            buttonIcon={icon}
            options={dropdownItems as string[]}
            selectedOption={selectedOption}
            testID="variable--tooltip-dropdown"
            buttonStatus={status}
            style={{width: '200px'}}
            onSelect={value => onSelectVariableValue(variableID, value)}
          />
        )}
      </Form.Element>
    </div>
  )
}

const mstp = (state: AppState, ownProps: OwnProps) => {
  const valuesStatus = getTimeMachineValuesStatus(state)
  const {variableID} = ownProps
  const values = getTimeMachineValues(state, variableID, ownProps.variable)
  return {values, valuesStatus}
}

const mdtp = {
  onAddVariableToTimeMachine: addVariableToTimeMachine,
  onSelectVariableValue: selectVariableValue,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VariableTooltipContents)
