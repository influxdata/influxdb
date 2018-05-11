import React, {PureComponent, ReactElement} from 'react'
import FuncArg from 'src/ifql/components/FuncArg'
import {OnChangeArg} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Func} from 'src/types/ifql'
import {funcNames} from 'src/ifql/constants'

interface Props {
  func: Func
  bodyID: string
  onChangeArg: OnChangeArg
  declarationID: string
  onGenerateScript: () => void
  onDeleteFunc: () => void
}

@ErrorHandling
export default class FuncArgs extends PureComponent<Props> {
  public render() {
    const {
      func,
      bodyID,
      onChangeArg,
      onDeleteFunc,
      declarationID,
      onGenerateScript,
    } = this.props

    return (
      <div className="func-node--tooltip">
        {func.args.map(({key, value, type}) => {
          return (
            <FuncArg
              key={key}
              type={type}
              argKey={key}
              value={value}
              bodyID={bodyID}
              funcID={func.id}
              funcName={func.name}
              onChangeArg={onChangeArg}
              declarationID={declarationID}
              onGenerateScript={onGenerateScript}
            />
          )
        })}
        <div className="func-node--buttons">
          <div
            className="btn btn-sm btn-danger func-node--delete"
            onClick={onDeleteFunc}
          >
            Delete
          </div>
          {this.build}
        </div>
      </div>
    )
  }

  get build(): ReactElement<HTMLDivElement> {
    const {func, onGenerateScript} = this.props
    if (func.name === funcNames.FILTER) {
      return (
        <div
          className="btn btn-sm btn-primary func-node--build"
          onClick={onGenerateScript}
        >
          Build
        </div>
      )
    }

    return null
  }
}
