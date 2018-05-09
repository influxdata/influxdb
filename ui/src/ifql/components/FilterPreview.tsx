import React, {PureComponent} from 'react'
import {BinaryExpressionNode, MemberExpressionNode} from 'src/types/ifql'

type FilterNode = BinaryExpressionNode | MemberExpressionNode

interface Props {
  nodes: FilterNode[]
}

class FilterPreview extends PureComponent<Props> {
  public render() {
    return this.props.nodes.map((n, i) => <div key={i}>{n.source}</div>)
  }
}

export default FilterPreview
