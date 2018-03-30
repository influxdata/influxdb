import React, {SFC} from 'react'

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

const Node: SFC<Props> = ({node}) => {
  return (
    <div className="func-node">
      <div>{node.name}</div>
    </div>
  )
}

export default Node
