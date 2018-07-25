import React, {PureComponent} from 'react'
import {MemberExpressionNode} from 'src/types/flux'

type FilterNode = MemberExpressionNode

interface Props {
  nodes: FilterNode[]
}

interface State {
  tags: Tags
}

interface Tags {
  [x: string]: string[]
}

export class FilterBuilder extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      tags: this.tags,
    }
  }

  public render() {
    return <div>Filter Builder</div>
  }

  private get tags(): Tags {
    const {nodes} = this.props
    return nodes.reduce((acc, node, i) => {
      if (node.type === 'MemberExpression') {
        const tagKey = node.property.name
        const remainingNodes = nodes.slice(i + 1, nodes.length)
        const tagValue = remainingNodes.find(n => {
          return n.type !== 'Operator'
        })

        if (!(tagKey in acc)) {
          return {...acc, [tagKey]: [tagValue.source]}
        }

        return {...acc, [tagKey]: [...acc[tagKey], tagValue.source]}
      }

      return acc
    }, {})
  }
}

export default FilterBuilder
