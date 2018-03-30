import React, {PureComponent, MouseEvent} from 'react'
import FuncArgs from 'src/ifql/components/FuncArgs'

export interface Params {
  [key: string]: string
}

export interface Func {
  name: string
  params: Params
}

interface Arg {
  key: string
  value: string
}

interface Node {
  name: string
  arguments: Arg[]
}

interface Props {
  node: Node
  func: Func
}

interface State {
  isOpen: boolean
}

export default class FuncNode extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
    }
  }

  public render() {
    const {node, func} = this.props
    const {isOpen} = this.state

    return (
      <div>
        <div className="func-node" onClick={this.handleClick}>
          <div>{node.name}</div>
        </div>
        {isOpen && <FuncArgs args={node.arguments} func={func} />}
      </div>
    )
  }

  private handleClick = (e: MouseEvent<HTMLElement>) => {
    e.stopPropagation()

    const {isOpen} = this.state
    this.setState({isOpen: !isOpen})
  }
}
