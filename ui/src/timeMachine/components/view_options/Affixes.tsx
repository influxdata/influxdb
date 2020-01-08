// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Form,
  Input,
  Grid,
  Toggle,
  InputToggleType,
  InputLabel,
  FlexBox,
  AlignItems,
  ComponentSize,
} from '@influxdata/clockface'

// Types
import {Columns} from '@influxdata/clockface'

interface Props {
  prefix: string
  tickPrefix: string
  suffix: string
  tickSuffix: string
  onUpdatePrefix: (prefix: string) => void
  onUpdateTickPrefix: (tickPrefix: string) => void
  onUpdateSuffix: (suffix: string) => void
  onUpdateTickSuffix: (tickSuffix: string) => void
}

class Affixes extends PureComponent<Props> {
  public render() {
    const {prefix, tickPrefix, suffix, tickSuffix} = this.props

    return (
      <>
        <Grid.Column widthXS={Columns.Six}>
          <Form.Element label="Prefix">
            <Input
              value={prefix}
              onChange={this.handleUpdatePrefix}
              placeholder="%, MPH, etc."
            />
          </Form.Element>
        </Grid.Column>
        <Grid.Column widthXS={Columns.Six}>
          <Form.Element label="Suffix">
            <Input
              value={suffix}
              onChange={this.handleUpdateSuffix}
              placeholder="%, MPH, etc."
            />
          </Form.Element>
        </Grid.Column>
        <Grid.Column widthXS={Columns.Six}>
          <FlexBox alignItems={AlignItems.Center} margin={ComponentSize.Small}>
            <Toggle
              id={'prefixoptional'}
              type={InputToggleType.Checkbox}
              value={tickPrefix}
              onChange={this.handleUpdateTickPrefix}
              size={ComponentSize.ExtraSmall}
            />
            <InputLabel active={!!tickPrefix}>Optional Prefix</InputLabel>
          </FlexBox>
        </Grid.Column>
        <Grid.Column widthXS={Columns.Six}>
          <FlexBox alignItems={AlignItems.Center} margin={ComponentSize.Small}>
            <Toggle
              id={'suffixoptional'}
              type={InputToggleType.Checkbox}
              value={tickSuffix}
              onChange={this.handleUpdateTickSuffix}
              size={ComponentSize.ExtraSmall}
            />
            <InputLabel active={!!tickSuffix}>Optional Suffix</InputLabel>
          </FlexBox>
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
  private handleUpdateTickSuffix = (e: string): void => {
    const {onUpdateTickSuffix} = this.props
    console.log(e)
    if (e === 'false' || !!!e) {
      onUpdateTickSuffix('true')
    } else {
      onUpdateTickSuffix('false')
    }
  }
  private handleUpdateTickPrefix = (e: string): void => {
    const {onUpdateTickPrefix} = this.props
    const tickPrefix = e
    onUpdateTickPrefix(tickPrefix)
  }
}

export default Affixes
