// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Form, Input, Grid} from '@influxdata/clockface'

// Types
import {Columns} from '@influxdata/clockface'

interface Props {
  prefix: string
  suffix: string
  onUpdatePrefix: (prefix: string) => void
  onUpdateSuffix: (suffix: string) => void
  testIDsuffix?: string,
  testIDprefix?: string,
}

class Affixes extends PureComponent<Props> {
  public render() {
    const {prefix, suffix, testIDprefix, testIDsuffix} = this.props

    return (
      <>
        <Grid.Column widthXS={Columns.Six}>
          <Form.Element label="Prefix">
            <Input
              value={prefix}
              onChange={this.handleUpdatePrefix}
              placeholder="%, MPH, etc."
              testID={testIDprefix || "input--prefix"}
            />
          </Form.Element>
        </Grid.Column>
        <Grid.Column widthXS={Columns.Six}>
          <Form.Element label="Suffix">
            <Input
              value={suffix}
              onChange={this.handleUpdateSuffix}
              placeholder="%, MPH, etc."
              testID={testIDsuffix || "input--suffix"}
            />
          </Form.Element>
        </Grid.Column>
      </>
    )
  }

  private handleUpdatePrefix = (e: ChangeEvent<HTMLInputElement>): void => {
    const {onUpdatePrefix} = this.props
    const prefix = e.target.value
    onUpdatePrefix(prefix)
  }

  private handleUpdateSuffix = (e: ChangeEvent<HTMLInputElement>): void => {
    const {onUpdateSuffix} = this.props
    const suffix = e.target.value
    onUpdateSuffix(suffix)
  }
}

export default Affixes
