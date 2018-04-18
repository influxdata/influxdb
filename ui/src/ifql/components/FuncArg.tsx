import React, {PureComponent} from 'react'

import FuncArgInput, {OnChangeArg} from 'src/ifql/components/FuncArgInput'
import FuncArgBool from 'src/ifql/components/FuncArgBool'
import * as types from 'src/ifql/constants/argumentTypes'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  funcID: string
  argKey: string
  value: string | boolean
  type: string
  onChangeArg: OnChangeArg
  onGenerateScript: () => void
}

@ErrorHandling
class FuncArg extends PureComponent<Props> {
  public render() {
    const {
      argKey,
      value,
      type,
      onChangeArg,
      funcID,
      onGenerateScript,
    } = this.props

    switch (type) {
      case types.STRING:
      case types.DURATION:
      case types.TIME:
      case types.REGEXP:
      case types.FLOAT:
      case types.INT:
      case types.UINT:
      case types.ARRAY: {
        return (
          <FuncArgInput
            type={type}
            value={`${value}`}
            argKey={argKey}
            funcID={funcID}
            onChangeArg={onChangeArg}
            onGenerateScript={onGenerateScript}
          />
        )
      }

      case types.BOOL: {
        return (
          <FuncArgBool
            value={this.boolValue}
            argKey={argKey}
            funcID={funcID}
            onChangeArg={onChangeArg}
            onGenerateScript={onGenerateScript}
          />
        )
      }
      case types.FUNCTION: {
        // TODO: make separate function component
        return (
          <div className="func-arg">
            {argKey} : {value}
          </div>
        )
      }
      case types.NIL: {
        // TODO: handle nil type
        return (
          <div className="func-arg">
            {argKey} : {value}
          </div>
        )
      }
      default: {
        return (
          <div className="func-arg">
            {argKey} : {value}
          </div>
        )
      }
    }
  }

  private get boolValue(): boolean {
    return this.props.value === true
  }
}

export default FuncArg
