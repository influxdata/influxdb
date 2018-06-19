import React, {PureComponent, MouseEvent} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import BodyDelete from 'src/flux/components/BodyDelete'
import FuncArgs from 'src/flux/components/FuncArgs'
import FuncArgsPreview from 'src/flux/components/FuncArgsPreview'
import {
  OnDeleteFuncNode,
  OnChangeArg,
  OnToggleYield,
  Func,
} from 'src/types/flux'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Service} from 'src/types'

interface Props {
  func: Func
  funcs: Func[]
  service: Service
  bodyID: string
  index: number
  declarationID?: string
  onDelete: OnDeleteFuncNode
  onToggleYield: OnToggleYield
  onChangeArg: OnChangeArg
  onGenerateScript: () => void
  onToggleYieldWithLast: (funcNodeIndex: number) => void
  declarationsFromBody: string[]
  isYielding: boolean
  isYieldable: boolean
  onDeleteBody: (bodyID: string) => void
}

interface State {
  editing: boolean
}

@ErrorHandling
export default class FuncNode extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    declarationID: '',
  }

  constructor(props) {
    super(props)

    this.state = {
      editing: this.isLast,
    }
  }

  public render() {
    const {func} = this.props

    return (
      <>
        <div className="func-node--wrapper">
          <div
            className={this.nodeClassName}
            onClick={this.handleToggleEdit}
            title="Edit function arguments"
          >
            <div className="func-node--connector" />
            <div className="func-node--name">{func.name}</div>
            <FuncArgsPreview func={func} />
          </div>
          {this.funcMenu}
        </div>
        {this.funcArgs}
      </>
    )
  }

  private get funcArgs(): JSX.Element {
    const {
      func,
      bodyID,
      service,
      isYielding,
      onChangeArg,
      declarationID,
      onGenerateScript,
      declarationsFromBody,
    } = this.props
    const {editing} = this.state

    if (!editing || isYielding) {
      return
    }

    return (
      <FuncArgs
        func={func}
        bodyID={bodyID}
        service={service}
        onChangeArg={onChangeArg}
        declarationID={declarationID}
        onGenerateScript={onGenerateScript}
        declarationsFromBody={declarationsFromBody}
        onStopPropagation={this.handleClickArgs}
      />
    )
  }

  private get funcMenu(): JSX.Element {
    return (
      <div className="func-node--menu">
        {this.yieldToggleButton}
        {this.deleteButton}
      </div>
    )
  }

  private get yieldToggleButton(): JSX.Element {
    const {isYielding} = this.props

    if (isYielding) {
      return (
        <button
          className="btn btn-sm btn-square btn-warning"
          onClick={this.handleToggleYield}
          title="Hide Data Table"
        >
          <span className="icon eye-closed" />
        </button>
      )
    }

    return (
      <button
        className="btn btn-sm btn-square btn-warning"
        onClick={this.handleToggleYield}
        title="See Data Table returned by this Function"
      >
        <span className="icon eye-open" />
      </button>
    )
  }

  private get deleteButton(): JSX.Element {
    const {func, bodyID, onDeleteBody} = this.props

    if (func.name === 'from') {
      return <BodyDelete onDeleteBody={onDeleteBody} bodyID={bodyID} />
    }

    return (
      <button
        className="btn btn-sm btn-square btn-danger"
        onClick={this.handleDelete}
        title="Delete this Function"
      >
        <span className="icon remove" />
      </button>
    )
  }

  private get nodeClassName(): string {
    const {isYielding} = this.props
    const {editing} = this.state

    return classnames('func-node', {active: isYielding || editing})
  }

  private handleDelete = (): void => {
    const {func, bodyID, declarationID} = this.props

    this.props.onDelete({funcID: func.id, bodyID, declarationID})
  }

  private handleToggleEdit = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()
    this.setState({editing: !this.state.editing})
  }

  private handleToggleYield = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()

    const {
      onToggleYield,
      index,
      bodyID,
      declarationID,
      isYieldable,
      onToggleYieldWithLast,
    } = this.props

    if (isYieldable) {
      onToggleYield(bodyID, declarationID, index)
    } else {
      onToggleYieldWithLast(index)
    }
  }

  private handleClickArgs = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()
  }

  private get isLast(): boolean {
    const {funcs, func} = this.props

    const lastFunc = _.last(funcs)

    return lastFunc.id === func.id
  }
}
