// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {
  Form,
  SelectDropdown,
  IconFont,
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
import {RemoteDataState, VariableValues} from 'src/types'
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
  const dropdownItems: string[] = get(values, 'values') || []

  const handleMouseEnter = () => {
    if (values || valuesStatus === RemoteDataState.Loading) {
      return
    }

    onAddVariableToTimeMachine(variableID)
  }

  let selectedOption = 'None Selected'
  let icon
  let status = toComponentStatus(valuesStatus)

  if (!values) {
    selectedOption = 'Failed to Load'
    icon = IconFont.AlertTriangle
    status = ComponentStatus.Disabled
  } else if (values.error) {
    selectedOption = 'Failed to Load'
    icon = IconFont.AlertTriangle
    status = ComponentStatus.Disabled
  } else if (!values.values.length) {
    selectedOption = 'No Results'
  } else {
    selectedOption = get(values, 'selectedValue', 'None Selected')
  }

  return (
    <div onMouseEnter={handleMouseEnter}>
      <Form.Element label="Value">
        <SelectDropdown
          buttonIcon={icon}
          options={dropdownItems}
          selectedOption={selectedOption}
          buttonStatus={status}
          style={{width: '200px'}}
          onSelect={value => onSelectVariableValue(variableID, value)}
        />
      </Form.Element>
    </div>
  )
}

const mstp = (state: AppState, ownProps: OwnProps) => {
  const valuesStatus = getTimeMachineValuesStatus(state)
  const values = getTimeMachineValues(state, ownProps.variableID)

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
