import React, {PureComponent, MouseEvent} from 'react'
import classnames from 'classnames'

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
  service: Service
  bodyID: string
  index: number
  declarationID?: string
  onDelete: OnDeleteFuncNode
  onToggleYield: OnToggleYield
  onChangeArg: OnChangeArg
  onGenerateScript: () => void
  declarationsFromBody: string[]
  isYielding: boolean
  isYieldable: boolean
}

interface State {
  isExpanded: boolean
}

@ErrorHandling
export default class FuncNode extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    declarationID: '',
  }

  constructor(props) {
    super(props)

    this.state = {
      isExpanded: false,
    }
  }

  public render() {
    const {
      func,
      bodyID,
      service,
      onChangeArg,
      declarationID,
      onGenerateScript,
      declarationsFromBody,
    } = this.props
    const {isExpanded} = this.state

    return (
      <div
        className={this.nodeClassName}
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
        onClick={this.handleClick}
      >
        <div className="func-node--connector" />
        <div className="func-node--name">{func.name}</div>
        <FuncArgsPreview func={func} />

        {isExpanded && (
          <FuncArgs
            func={func}
            bodyID={bodyID}
            service={service}
            onChangeArg={onChangeArg}
            declarationID={declarationID}
            onGenerateScript={onGenerateScript}
            onDeleteFunc={this.handleDelete}
            declarationsFromBody={declarationsFromBody}
            onStopPropagation={this.handleClickArgs}
          />
        )}
      </div>
    )
  }

  private get nodeClassName(): string {
    const {isYielding} = this.props
    return classnames('func-node', {active: isYielding})
  }

  private handleDelete = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()
    const {func, bodyID, declarationID} = this.props

    this.props.onDelete({funcID: func.id, bodyID, declarationID})
  }

  private handleMouseEnter = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()

    this.setState({isExpanded: true})
  }

  private handleMouseLeave = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()

    this.setState({isExpanded: false})
  }

  private handleClick = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()

    const {
      onToggleYield,
      index,
      bodyID,
      declarationID,
      isYieldable,
    } = this.props

    if (isYieldable) {
      onToggleYield(bodyID, declarationID, index)
    }
  }
  private handleClickArgs = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()
  }
}
