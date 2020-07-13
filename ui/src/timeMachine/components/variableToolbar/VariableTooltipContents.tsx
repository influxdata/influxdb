// Libraries
import React, {FunctionComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Actions
import {executeQueries} from 'src/timeMachine/actions/queries'

// Components
import {Form} from '@influxdata/clockface'
import VariableDropdown from 'src/variables/components/VariableDropdown'

interface OwnProps {
  variableID: string
}

type ReduxProps = ConnectedProps<typeof connector>

type Props = ReduxProps & OwnProps

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

const connector = connect(null, mdtp)

export default connector(VariableTooltipContents)
