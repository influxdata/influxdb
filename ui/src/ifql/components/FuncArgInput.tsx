import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'

export type OnChangeArg = (inputArg: InputArg) => void

export interface InputArg {
  funcID: string
  key: string
  value: string
}

interface Props {
  funcID: string
  argKey: string
  value: string
  type: string
  onChangeArg: OnChangeArg
  onGenerateScript: () => void
}

@ErrorHandling
class FuncArgInput extends PureComponent<Props> {
  public render() {
    const {argKey, value, type} = this.props
    return (
      <div>
        <label htmlFor={argKey}>{argKey}: </label>
        <input
          name={argKey}
          value={value}
          placeholder={type}
          onChange={this.handleChange}
          onKeyDown={this.handleKeyDown}
          type="text"
          className="form-control input-xs"
          spellCheck={false}
          autoComplete="off"
        />
      </div>
    )
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key !== 'Enter') {
      return
    }

    this.props.onGenerateScript()
    e.preventDefault()
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {funcID, argKey} = this.props

    this.props.onChangeArg({funcID, key: argKey, value: e.target.value})
  }
}

export default FuncArgInput
