import React, {PureComponent} from 'react'

import FuncArgInput from 'src/ifql/components/FuncArgInput'
import FuncArgBool from 'src/ifql/components/FuncArgBool'
import {ErrorHandling} from 'src/shared/decorators/errors'
import From from 'src/ifql/components/From'

import {funcNames, argTypes} from 'src/ifql/constants'
import {OnChangeArg} from 'src/types/ifql'

interface Props {
  funcName: string
  funcID: string
  argKey: string
  value: string | boolean
  type: string
  expressionID: string
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
      funcName,
      funcID,
      onChangeArg,
      expressionID,
      onGenerateScript,
    } = this.props

    if (funcName === funcNames.FROM) {
      return (
        <From
          argKey={argKey}
          funcID={funcID}
          value={this.value}
          expressionID={expressionID}
          onChangeArg={onChangeArg}
        />
      )
    }

    switch (type) {
      case argTypes.STRING:
      case argTypes.DURATION:
      case argTypes.TIME:
      case argTypes.REGEXP:
      case argTypes.FLOAT:
      case argTypes.INT:
      case argTypes.UINT:
      case argTypes.ARRAY: {
        return (
          <FuncArgInput
            type={type}
            value={this.value}
            argKey={argKey}
            funcID={funcID}
            expressionID={expressionID}
            onChangeArg={onChangeArg}
            onGenerateScript={onGenerateScript}
          />
        )
      }

      case argTypes.BOOL: {
        return (
          <FuncArgBool
            value={this.boolValue}
            argKey={argKey}
            funcID={funcID}
            onChangeArg={onChangeArg}
            expressionID={expressionID}
            onGenerateScript={onGenerateScript}
          />
        )
      }
      case argTypes.FUNCTION: {
        // TODO: make separate function component
        return (
          <div className="func-arg">
            {argKey} : {value}
          </div>
        )
      }
      case argTypes.NIL: {
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

  private get value(): string {
    return this.props.value.toString()
  }

  private get boolValue(): boolean {
    return this.props.value === true
  }
}

export default FuncArg
