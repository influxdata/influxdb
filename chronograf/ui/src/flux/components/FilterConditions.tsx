import React, {PureComponent} from 'react'
import {FilterNode} from 'src/types/flux'
import FilterConditionNode from 'src/flux/components/FilterConditionNode'

interface Props {
  nodes: FilterNode[]
}

class FilterConditions extends PureComponent<Props> {
  public render() {
    return (
      <>
        {this.props.nodes.map((n, i) => (
          <FilterConditionNode node={n} key={i} />
        ))}
      </>
    )
  }
}

export default FilterConditions
