import React, {PureComponent, ReactElement} from 'react'
import FuncArg from 'src/flux/components/FuncArg'
import {OnChangeArg} from 'src/types/flux'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Func} from 'src/types/flux'
import {funcNames} from 'src/flux/constants'
import Join from 'src/flux/components/Join'
import {Service} from 'src/types'

interface Props {
  func: Func
  service: Service
  bodyID: string
  onChangeArg: OnChangeArg
  declarationID: string
  onGenerateScript: () => void
  onDeleteFunc: () => void
  declarationsFromBody: string[]
}

@ErrorHandling
export default class FuncArgs extends PureComponent<Props> {
  public render() {
    const {
      func,
      bodyID,
      service,
      onChangeArg,
      onDeleteFunc,
      declarationID,
      onGenerateScript,
      declarationsFromBody,
    } = this.props
    const {name: funcName, id: funcID} = func
    return (
      <div className="func-node--tooltip">
        <div className="func-args">
          {funcName === funcNames.JOIN ? (
            <Join
              func={func}
              bodyID={bodyID}
              declarationID={declarationID}
              onChangeArg={onChangeArg}
              declarationsFromBody={declarationsFromBody}
              onGenerateScript={onGenerateScript}
            />
          ) : (
            func.args.map(({key, value, type}) => (
              <FuncArg
                key={key}
                type={type}
                argKey={key}
                value={value}
                bodyID={bodyID}
                funcID={funcID}
                funcName={funcName}
                service={service}
                onChangeArg={onChangeArg}
                declarationID={declarationID}
                onGenerateScript={onGenerateScript}
              />
            ))
          )}
        </div>
        <div className="func-arg--buttons">
          <div
            className="btn btn-sm btn-danger btn-square"
            onClick={onDeleteFunc}
          >
            <span className="icon trash" />
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
