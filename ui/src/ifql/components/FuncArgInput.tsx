import React, {PureComponent, ChangeEvent} from 'react'

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
}

class FuncArgInput extends PureComponent<Props> {
  public render() {
    const {argKey, value, type} = this.props
    return (
      <div>
        <label htmlFor={argKey}>{argKey}: </label>
        <input
          type="text"
          name={argKey}
          className="form-control input-xs"
          placeholder={type}
          spellCheck={false}
          autoComplete="off"
          value={value}
          onChange={this.handleChange}
        />
      </div>
    )
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {funcID, argKey} = this.props
    console.log(this.props)
    this.props.onChangeArg({funcID, key: argKey, value: e.target.value})
  }
}

export default FuncArgInput
