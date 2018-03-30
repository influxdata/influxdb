import React, {PureComponent} from 'react'

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
}

interface State {
  isOpen: boolean
}

export default class FuncNode extends PureComponent<Props, State> {
  public render() {
    const {node} = this.props

    return (
      <div className="func-node">
        <div>{node.name}</div>
      </div>
    )
  }
}
