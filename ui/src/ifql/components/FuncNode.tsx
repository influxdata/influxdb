import React, {PureComponent, MouseEvent} from 'react'
import FuncArgs from 'src/ifql/components/FuncArgs'
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
  isOpen: boolean
}

@ErrorHandling
export default class FuncNode extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    declarationID: '',
  }

  constructor(props) {
    super(props)
    this.state = {
      isOpen: true,
    }
  }

  public render() {
    const {bodyID, func, onChangeArg, onGenerateScript} = this.props
    const {isOpen} = this.state

    return (
      <div className="func-node">
        <div className="func-node--name" onClick={this.handleClick}>
          <div>{func.name}</div>
        </div>
        {isOpen && (
          <FuncArgs
            func={func}
            bodyID={bodyID}
            onChangeArg={onChangeArg}
            onGenerateScript={onGenerateScript}
          />
        )}
        <div className="btn btn-danger btn-square" onClick={this.handleDelete}>
          <span className="icon-trash" />
        </div>
      </div>
    )
  }

  private handleDelete = (): void => {
    const {func, bodyID, declarationID} = this.props

    this.props.onDelete({funcID: func.id, bodyID, declarationID})
  }

  private handleClick = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()

    const {isOpen} = this.state
    this.setState({isOpen: !isOpen})
  }
}
