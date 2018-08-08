import React, {PureComponent, ReactElement, MouseEvent} from 'react'
import FuncArg from 'src/flux/components/FuncArg'
import {OnChangeArg} from 'src/types/flux'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Func, OnGenerateScript} from 'src/types/flux'
import {funcNames} from 'src/flux/constants'
import JoinArgs from 'src/flux/components/JoinArgs'
import FilterArgs from 'src/flux/components/FilterArgs'
import {Source} from 'src/types/v2'
import {getDeep} from 'src/utils/wrappers'

interface Props {
  func: Func
  source: Source
  bodyID: string
  onChangeArg: OnChangeArg
  declarationID: string
  onGenerateScript: OnGenerateScript
  declarationsFromBody: string[]
  onStopPropagation: (e: MouseEvent<HTMLElement>) => void
}

@ErrorHandling
export default class FuncArgs extends PureComponent<Props> {
  public render() {
    const {onStopPropagation} = this.props

    return (
      <div className="func-node--editor" onClick={onStopPropagation}>
        <div className="func-node--connector" />
        <div className="func-args">{this.renderArguments}</div>
        <div className="func-arg--buttons">{this.build}</div>
      </div>
    )
  }

  get renderArguments(): JSX.Element | JSX.Element[] {
    const {func} = this.props
    const {name: funcName} = func

    if (funcName === funcNames.JOIN) {
      return this.renderJoin
    }

    if (funcName === funcNames.FILTER) {
      return this.renderFilter
    }

    return this.renderGeneralArguments
  }

  get renderGeneralArguments(): JSX.Element | JSX.Element[] {
    const {
      func,
      bodyID,
      source,
      onChangeArg,
      declarationID,
      onGenerateScript,
    } = this.props

    const {name: funcName, id: funcID, args} = func

    return args.map(({key, value, type}) => (
      <FuncArg
        key={key}
        args={args}
        type={type}
        argKey={key}
        value={value}
        bodyID={bodyID}
        funcID={funcID}
        funcName={funcName}
        source={source}
        onChangeArg={onChangeArg}
        declarationID={declarationID}
        onGenerateScript={onGenerateScript}
      />
    ))
  }

  get renderFilter(): JSX.Element {
    const {
      func,
      bodyID,
      source,
      onChangeArg,
      declarationID,
      onGenerateScript,
    } = this.props
    const value = getDeep<string>(func.args, '0.value', '')

    return (
      <FilterArgs
        value={value}
        func={func}
        bodyID={bodyID}
        declarationID={declarationID}
        onChangeArg={onChangeArg}
        onGenerateScript={onGenerateScript}
        source={source}
        db={'telegraf'}
      />
    )
  }

  get renderJoin(): JSX.Element {
    const {
      func,
      bodyID,
      onChangeArg,
      declarationID,
      onGenerateScript,
      declarationsFromBody,
    } = this.props

    return (
      <JoinArgs
        func={func}
        bodyID={bodyID}
        declarationID={declarationID}
        onChangeArg={onChangeArg}
        declarationsFromBody={declarationsFromBody}
        onGenerateScript={onGenerateScript}
      />
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
