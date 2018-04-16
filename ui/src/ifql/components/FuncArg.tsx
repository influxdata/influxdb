import React, {PureComponent} from 'react'

import FuncArgInput, {OnChangeArg} from 'src/ifql/components/FuncArgInput'
import * as types from 'src/ifql/constants/argumentTypes'

interface Props {
  funcID: string
  argKey: string
  value: string
  type: string
  onChangeArg: OnChangeArg
  onGenerateScript: () => void
}

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

    switch (true) {
      case this.isInput: {
        return (
          <FuncArgInput
            type={type}
            value={value}
            argKey={argKey}
            funcID={funcID}
            onChangeArg={onChangeArg}
            onGenerateScript={onGenerateScript}
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

    return (
      type === types.STRING ||
      type === types.DURATION ||
      type === types.TIME ||
      type === types.INT ||
      type === types.REGEXP ||
      type === types.UINT
    )
  }
}

export default FuncArg
