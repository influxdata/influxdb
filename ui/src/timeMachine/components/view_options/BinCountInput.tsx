// Libraries
import React, {Component, ChangeEvent} from 'react'

// Components
import {AutoInput, AutoInputMode, Input, InputType} from '@influxdata/clockface'

// Actions
import {setBinCount} from 'src/timeMachine/actions'

interface Props {
  binCount?: number
  onSetBinCount: typeof setBinCount
}

class BinCountInput extends Component<Props> {
  render() {
    const {binCount} = this.props

    return (
      <AutoInput
        mode={binCount ? AutoInputMode.Custom : AutoInputMode.Auto}
        onChangeMode={this.handleChangeMode}
        inputComponent={
          <Input
            name="binCount"
            value={binCount}
            placeholder="Enter a number"
            type={InputType.Number}
            min={0}
            onChange={this.handleInputChange}
          />
        }
      />
    )
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    const {onSetBinCount} = this.props

    const binCount = Number(e.target.value) || null

    onSetBinCount(binCount)
  }

  private handleChangeMode = (mode: AutoInputMode): void => {
    const {onSetBinCount} = this.props

    if (mode === AutoInputMode.Auto) {
      onSetBinCount(null)
    } else {
      onSetBinCount(30)
    }
  }
}

export default BinCountInput
