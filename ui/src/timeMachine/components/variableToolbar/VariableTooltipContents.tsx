// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {executeQueries} from 'src/timeMachine/actions/queries'

// Components
import {
  Form,
  DropdownMenuTheme,
  Dropdown,
  ComponentStatus,
} from '@influxdata/clockface'
import VariableDropdown from 'src/variables/components/VariableDropdown'

// Utils
import {getTimeMachineValuesStatus} from 'src/variables/selectors'
import {getVariable} from 'src/variables/selectors'
import {toComponentStatus} from 'src/shared/utils/toComponentStatus'

// Types
import {RemoteDataState, Variable} from 'src/types'
import {AppState} from 'src/types'

interface StateProps {
  timeMachineID: string
  valuesStatus: RemoteDataState
}

interface DispatchProps {
  execute: typeof executeQueries
}

interface OwnProps {
  variableID: string
}

type Props = StateProps & DispatchProps & OwnProps

const VariableTooltipContents: FunctionComponent<Props> = ({
  variableID,
  timeMachineID,
  valuesStatus,
  onAddVariableToTimeMachine,
  execute,
}) => {
  const refresh = () => {
    execute()
  }
  return (
    <div>
      <Form.Element label="Value">
        <VariableDropdown
          variableID={variableID}
          contextID={timeMachineID}
          onSelect={refresh}
          testID="variable--tooltip-dropdown"
        />
      </Form.Element>
    </div>
  )
}

const mstp = (state: AppState) => {
  const valuesStatus = getTimeMachineValuesStatus(state)
  const activeTimeMachineID = state.timeMachines.activeTimeMachineID

  return {
    timeMachineID: activeTimeMachineID,
    valuesStatus,
  }
}

const mdtp = {
  execute: executeQueries,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VariableTooltipContents)
