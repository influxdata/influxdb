import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {OnChangeArg, OnGenerateScript} from 'src/types/flux'

interface Props {
  funcID: string
  argKey: string
  value: string
  type: string
  bodyID: string
  declarationID: string
  onChangeArg: OnChangeArg
  onGenerateScript: OnGenerateScript
  autoFocus?: boolean
}

@ErrorHandling
class FuncArgInput extends PureComponent<Props> {
  public render() {
    const {argKey, value, type, autoFocus} = this.props

    return (
      <div className="func-arg">
        <label className="func-arg--label" htmlFor={argKey}>
          {argKey}
        </label>
        <div className="func-arg--value">
          <input
            name={argKey}
            value={value}
            placeholder={type}
            onChange={this.handleChange}
            onKeyDown={this.handleKeyDown}
            type="text"
            className="form-control input-sm"
            spellCheck={false}
            autoComplete="off"
            autoFocus={autoFocus}
          />
        </div>
      </div>
    )
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key !== 'Enter') {
      return
    }

    e.preventDefault()
    this.props.onGenerateScript()
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {funcID, argKey, bodyID, declarationID} = this.props

    this.props.onChangeArg({
      funcID,
      key: argKey,
      value: e.target.value,
      declarationID,
      bodyID,
    })
  }
}

export default FuncArgInput
