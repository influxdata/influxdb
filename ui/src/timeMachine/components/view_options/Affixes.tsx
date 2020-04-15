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
  type: string
  onUpdatePrefix: (prefix: string) => void
  onUpdateTickPrefix: (tickPrefix: string) => void
  onUpdateSuffix: (suffix: string) => void
  onUpdateTickSuffix: (tickSuffix: string) => void
}

class Affixes extends PureComponent<Props> {
  public render() {
    const {prefix, suffix} = this.props

    return (
      <>
        <Grid.Column widthXS={Columns.Six}>
          <Form.Element label="Prefix">
            <Input
              testID="prefix-input"
              value={prefix}
              onChange={this.handleUpdatePrefix}
              placeholder="%, MPH, etc."
            />
          </Form.Element>
        </Grid.Column>
        <Grid.Column widthXS={Columns.Six}>
          <Form.Element label="Suffix">
            <Input
              testID="suffix-input"
              value={suffix}
              onChange={this.handleUpdateSuffix}
              placeholder="%, MPH, etc."
            />
          </Form.Element>
        </Grid.Column>
        {this.optionalTicks}
      </>
    )
  }

  private get optionalTicks(): JSX.Element {
    const {type, tickPrefix, tickSuffix} = this.props

    if (type === 'single-stat') {
      return null
    } else {
      return (
        <>
          <Grid.Column widthXS={Columns.Six}>
            <FlexBox
              alignItems={AlignItems.Center}
              margin={ComponentSize.Small}
              className="view-options--checkbox"
            >
              <Toggle
                id="prefixoptional"
                testID="tickprefix-input"
                type={InputToggleType.Checkbox}
                value={tickPrefix}
                onChange={this.handleUpdateTickPrefix}
                size={ComponentSize.ExtraSmall}
              />
              <InputLabel active={!!tickPrefix}>Optional Prefix</InputLabel>
            </FlexBox>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Six}>
            <FlexBox
              alignItems={AlignItems.Center}
              margin={ComponentSize.Small}
              className="view-options--checkbox"
            >
              <Toggle
                id="suffixoptional"
                testID="ticksuffix-input"
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
    if (e === 'false' || !!!e) {
      onUpdateTickSuffix('true')
    } else {
      onUpdateTickSuffix('false')
    }
  }
  private handleUpdateTickPrefix = (e: string): void => {
    const {onUpdateTickPrefix} = this.props
    if (e === 'false' || !!!e) {
      onUpdateTickPrefix('true')
    } else {
      onUpdateTickPrefix('false')
    }
  }
}

export default Affixes
