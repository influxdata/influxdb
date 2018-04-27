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
    const {
      func,
      bodyID,
      onChangeArg,
      declarationID,
      onGenerateScript,
    } = this.props
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
            declarationID={declarationID}
            onGenerateScript={onGenerateScript}
          />
        )}
        <div className="func-node--delete" onClick={this.handleDelete} />
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
