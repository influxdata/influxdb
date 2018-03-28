import React, {SFC} from 'react'
import FuncSelector from 'src/ifql/components/FuncSelector'
import Node from 'src/ifql/components/Node'

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
      {nodes.map((n, i) => <Node key={i} node={n} />)}
      <FuncSelector funcs={funcs} onAddNode={onAddNode} />
    </div>
  )
}

export default TimeMachine
