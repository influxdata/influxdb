// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {executeQueries} from 'src/timeMachine/actions/queries'

// Components
import {Form} from '@influxdata/clockface'
import VariableDropdown from 'src/variables/components/VariableDropdown'

interface DispatchProps {
  execute: typeof executeQueries
}

interface OwnProps {
  variableID: string
}

type Props = DispatchProps & OwnProps

const VariableTooltipContents: FunctionComponent<Props> = ({
  variableID,
  execute,
}) => {
  const refresh = () => {
    execute()
  }
  return (
    <div
      className="flux-toolbar--popover"
      data-testid="flux-toolbar--variable-popover"
    >
      <Form.Element label="Value">
        <VariableDropdown
          variableID={variableID}
          onSelect={refresh}
          testID="variable--tooltip-dropdown"
        />
      </Form.Element>
    </div>
  )
}

const mdtp = {
  execute: executeQueries,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(VariableTooltipContents)
