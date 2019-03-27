// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {Form} from '@influxdata/clockface'
import {Dropdown} from 'src/clockface'

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
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types'
import {VariableValues} from 'src/variables/types'

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

  return (
    <div className="variable-tooltip--contents" onMouseEnter={handleMouseEnter}>
      <Form.Element label="Value">
        <Dropdown
          selectedID={get(values, 'selectedValue')}
          status={toComponentStatus(valuesStatus)}
          titleText={values ? 'No Results' : 'None Selected'}
          widthPixels={200}
          onChange={value => onSelectVariableValue(variableID, value)}
        >
          {dropdownItems.map(value => (
            <Dropdown.Item id={value} key={value} value={value}>
              {value}
            </Dropdown.Item>
          ))}
        </Dropdown>
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
