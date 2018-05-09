import React, {PureComponent} from 'react'
import {BinaryExpressionNode, MemberExpressionNode} from 'src/types/ifql'

type FilterNode = BinaryExpressionNode | MemberExpressionNode

interface Props {
  nodes: FilterNode[]
}

class FilterPreview extends PureComponent<Props> {
  public render() {
    return (
      <div style={{display: 'flex'}}>
        {this.props.nodes.map((n, i) => <FilterPreviewNode node={n} key={i} />)}
      </div>
    )
  }
}

interface FilterPreviewNodeProps {
  node: FilterNode
}

/* tslint:disable */
class FilterPreviewNode extends PureComponent<FilterPreviewNodeProps> {
  public render() {
    const {node} = this.props
    return <div className={this.className}>{node.source}</div>
  }

  private get className(): string {
    const {type} = this.props.node

    switch (type) {
      case 'ObjectExpression':
      case 'MemberExpression': {
        return 'ifql-filter--expression'
      }
      case 'OpenParen':
      case 'CloseParen': {
        return 'ifql-filter--paren'
      }
      case 'NumberLiteral': {
        return 'variable-value--number'
      }
      case 'BooleanLiteral': {
        return 'variable-value--boolean'
      }
      case 'StringLiteral': {
        return 'variable-value--string'
      }
      case 'Operator': {
        return 'ifql-filter--operator'
      }
      default: {
        return ''
      }
    }
  }
}

export default FilterPreview
