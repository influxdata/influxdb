import React, {SFC} from 'react'
import FuncSelector from 'src/ifql/components/FuncSelector'
import FuncNode from 'src/ifql/components/FuncNode'

interface Arg {
  key: string
  value: string
}

interface NodeProp {
  name: string
  arguments: Arg[]
}

interface Props {
  funcs: string[]
  nodes: NodeProp[]
  onAddNode: (name: string) => void
}

const TimeMachine: SFC<Props> = ({funcs, nodes, onAddNode}) => {
  return (
    <div>
      <div className="func-node-container">
        {nodes.map((n, i) => <FuncNode key={i} node={n} />)}
        <FuncSelector funcs={funcs} onAddNode={onAddNode} />
      </div>
    </div>
  )
}

export default TimeMachine
