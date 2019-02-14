// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  IndexList,
  Alignment,
  ComponentSize,
  ConfirmationButton,
} from 'src/clockface'

// Types
import {Macro as Variable} from '@influxdata/influx'

interface Props {
  variable: Variable
  onDeleteVariable: (variable: Variable) => void
}

export default class VariableRow extends PureComponent<Props> {
  public render() {
    const {variable, onDeleteVariable} = this.props

    return (
      <IndexList.Row>
        <IndexList.Cell alignment={Alignment.Left}>
          {variable.name}
        </IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Left}>{'Query'}</IndexList.Cell>
        <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
          <ConfirmationButton
            size={ComponentSize.ExtraSmall}
            text="Delete"
            confirmText="Confirm"
            onConfirm={onDeleteVariable}
            returnValue={variable}
          />
        </IndexList.Cell>
      </IndexList.Row>
    )
  }
}
