import React, {PureComponent, MouseEvent} from 'react'
import FuncArgs from 'src/ifql/components/FuncArgs'
import {Func} from 'src/ifql/components/FuncArgs'
import {OnChangeArg} from 'src/ifql/components/FuncArgInput'

interface Props {
  func: Func
  onDelete: (id: string) => void
  onChangeArg: OnChangeArg
}

interface State {
  isOpen: boolean
}

export default class FuncNode extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: true,
    }
  }

  public render() {
    const {func, onChangeArg} = this.props
    const {isOpen} = this.state

    return (
      <div className="func-node">
        <div className="func-node--name" onClick={this.handleClick}>
          <div>{func.name}</div>
        </div>
        {isOpen && <FuncArgs func={func} onChangeArg={onChangeArg} />}
        <div className="btn btn-danger btn-square" onClick={this.handleDelete}>
          <span className="icon-trash" />
        </div>
      </div>
    )
  }

  private handleDelete = (): void => {
    this.props.onDelete(this.props.func.id)
  }

  private handleClick = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()

    const {isOpen} = this.state
    this.setState({isOpen: !isOpen})
  }
}
