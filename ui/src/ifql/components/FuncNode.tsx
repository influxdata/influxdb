import React, {PureComponent, MouseEvent} from 'react'

import FuncArgs from 'src/ifql/components/FuncArgs'
import FuncArgsPreview from 'src/ifql/components/FuncArgsPreview'
import {OnDeleteFuncNode, OnChangeArg, Func} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  func: Func
  bodyID: string
  declarationID?: string
  onDelete: OnDeleteFuncNode
  onChangeArg: OnChangeArg
  onGenerateScript: () => void
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
      func: {args},
      bodyID,
      onChangeArg,
      declarationID,
      onGenerateScript,
    } = this.props
    const {isExpanded} = this.state

    return (
      <div
        className="func-node"
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
      >
        <div className="func-node--name">{func.name}</div>
        <FuncArgsPreview func={func} />
        {isExpanded && (
          <FuncArgs
            func={func}
            bodyID={bodyID}
            onChangeArg={onChangeArg}
            declarationID={declarationID}
            onGenerateScript={onGenerateScript}
            onDeleteFunc={this.handleDelete}
          />
        )}
      </div>
    )
  }

  private handleDelete = (): void => {
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
}
