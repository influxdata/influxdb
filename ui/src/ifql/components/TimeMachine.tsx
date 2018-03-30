import React, {PureComponent} from 'react'
import FuncSelector from 'src/ifql/components/FuncSelector'
import FuncNode from 'src/ifql/components/FuncNode'

import {Func} from 'src/ifql/components/FuncNode'

interface Arg {
  key: string
  value: string
}

interface NodeProp {
  name: string
  arguments: Arg[]
}

interface Props {
  funcs: Func[]
  nodes: NodeProp[]
  onAddNode: (name: string) => void
}

class TimeMachine extends PureComponent<Props> {
  public render() {
    const {nodes, onAddNode} = this.props

    return (
      <div>
        <div className="func-node-container">
          {nodes.map((n, i) => (
            <FuncNode key={i} node={n} func={this.getFunc(n.name)} />
          ))}
          <FuncSelector funcs={this.funcNames} onAddNode={onAddNode} />
        </div>
      </div>
    )
  }

  private get funcNames() {
    return this.props.funcs.map(f => f.name)
  }

  private getFunc(name) {
    return this.props.funcs.find(f => f.name === name)
  }
}

export default TimeMachine
