// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Input, FormElement, Grid} from '@influxdata/clockface'

// Types
import {Columns} from '@influxdata/clockface'

interface Props {
  prefix: string
  suffix: string
  axisName: string
  onUpdateAxisPrefix: (prefix: string) => void
  onUpdateAxisSuffix: (suffix: string) => void
}

class AxisAffixes extends PureComponent<Props> {
  public render() {
    const {prefix, suffix, axisName} = this.props

    return (
      <>
        <Grid.Column widthSM={Columns.Six}>
          <FormElement label={`${axisName.toUpperCase()} Axis Prefix`}>
            <Input value={prefix} onChange={this.handleUpdateAxisPrefix} />
          </FormElement>
        </Grid.Column>
        <Grid.Column widthSM={Columns.Six}>
          <FormElement label={`${axisName.toUpperCase()} Axis Suffix`}>
            <Input value={suffix} onChange={this.handleUpdateAxisSuffix} />
          </FormElement>
        </Grid.Column>
      </>
    )
  }

  private handleUpdateAxisPrefix = (e: ChangeEvent<HTMLInputElement>): void => {
    const {onUpdateAxisPrefix} = this.props
    const prefix = e.target.value
    onUpdateAxisPrefix(prefix)
  }

  private handleUpdateAxisSuffix = (e: ChangeEvent<HTMLInputElement>): void => {
    const {onUpdateAxisSuffix} = this.props
    const suffix = e.target.value
    onUpdateAxisSuffix(suffix)
  }
}

export default AxisAffixes
