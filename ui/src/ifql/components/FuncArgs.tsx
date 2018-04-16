import React, {PureComponent} from 'react'
import FuncArg from 'src/ifql/components/FuncArg'
import {OnChangeArg} from 'src/ifql/components/FuncArgInput'

interface Arg {
  key: string
  value: string
  type: string
}

export interface Func {
  name: string
  args: Arg[]
  source: string
  id: string
}

interface Props {
  func: Func
  onChangeArg: OnChangeArg
}

export default class FuncArgs extends PureComponent<Props> {
  public render() {
    const {func, onChangeArg} = this.props

    return (
      <div className="func-args">
        {func.args.map(({key, value, type}) => {
          return (
            <FuncArg
              funcID={func.id}
              key={key}
              argKey={key}
              value={value}
              type={type}
              onChangeArg={onChangeArg}
            />
          )
        })}
      </div>
    )
  }
}
