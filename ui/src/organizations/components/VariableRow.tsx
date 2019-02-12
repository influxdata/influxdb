// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList, Alignment} from 'src/clockface'

// Types
import {Macro} from '@influxdata/influx'

interface Props {
  variable: Macro
}

export default class VariableRow extends PureComponent<Props> {
  public render() {
    const {variable} = this.props
    return (
      <IndexList.Row>
        <IndexList.Cell alignment={Alignment.Left}>
          {variable.name}
        </IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Left}>{'Query'}</IndexList.Cell>
      </IndexList.Row>
    )
  }
}
