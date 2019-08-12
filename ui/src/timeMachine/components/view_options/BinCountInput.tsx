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

interface State {
  mode: AutoInputMode
}

class BinCountInput extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      mode: this.props.binCount ? AutoInputMode.Custom : AutoInputMode.Auto,
    }
  }

  render() {
    const {binCount} = this.props
    const {mode} = this.state

    return (
      <AutoInput
        mode={mode}
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

    const binCount = Number(e.target.value) || 0

    onSetBinCount(binCount)
  }

  private handleChangeMode = (mode: AutoInputMode): void => {
    this.setState({mode})
  }
}

export default BinCountInput
