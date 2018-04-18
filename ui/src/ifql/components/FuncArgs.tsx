import React, {PureComponent} from 'react'
import FuncArg from 'src/ifql/components/FuncArg'
import {OnChangeArg} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'

type Value = string | boolean

interface Arg {
  key: string
  value: Value
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
  expressionID: string
  onChangeArg: OnChangeArg
  onGenerateScript: () => void
}

@ErrorHandling
export default class FuncArgs extends PureComponent<Props> {
  public render() {
    const {expressionID, func, onChangeArg, onGenerateScript} = this.props

    return (
      <div className="func-args">
        {func.args.map(({key, value, type}) => {
          return (
            <FuncArg
              key={key}
              type={type}
              argKey={key}
              value={value}
              funcID={func.id}
              funcName={func.name}
              onChangeArg={onChangeArg}
              expressionID={expressionID}
              onGenerateScript={onGenerateScript}
            />
          )
        })}
      </div>
    )
  }
}
