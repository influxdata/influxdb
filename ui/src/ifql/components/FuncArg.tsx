import React, {PureComponent} from 'react'

import FuncArgInput, {OnChangeArg} from 'src/ifql/components/FuncArgInput'
import * as types from 'src/ifql/constants/argumentTypes'

interface Props {
  funcID: string
  argKey: string
  value: string
  type: string
  onChangeArg: OnChangeArg
}

class FuncArg extends PureComponent<Props> {
  public render() {
    const {argKey, value, type, onChangeArg, funcID} = this.props

    switch (true) {
      case this.isInput: {
        return (
          <FuncArgInput
            funcID={funcID}
            type={type}
            value={value}
            argKey={argKey}
            onChangeArg={onChangeArg}
          />
        )
      }
      case types.BOOL === type: {
        // TODO: make boolean arg component
        return (
          <div className="func-arg">
            {argKey} : {value}
          </div>
        )
      }
      case types.FUNCTION === type: {
        // TODO: make separate function component
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

  private get isInput() {
    const {type} = this.props

    return type !== types.FUNCTION || types.NIL || types.BOOL || types.INVALID
  }
}

export default FuncArg
