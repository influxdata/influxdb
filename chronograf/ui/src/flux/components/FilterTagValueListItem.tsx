import React, {PureComponent, MouseEvent} from 'react'

import {SetFilterTagValue} from 'src/types/flux'

interface Props {
  tagKey: string
  value: string
  onChangeValue: SetFilterTagValue
  selected: boolean
}

class FilterTagValueListItem extends PureComponent<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {value} = this.props

    return (
      <div
        className="flux-schema-tree flux-schema--child"
        onClick={this.handleClick}
      >
        <div className={this.listItemClasses}>
          <div className="flex-schema-item-group">
            <div className="query-builder--checkbox" />
            {value}
            <span className="flux-schema--type">Tag Value</span>
          </div>
        </div>
      </div>
    )
  }

  private handleClick = (e: MouseEvent<HTMLDivElement>) => {
    const {tagKey, value, selected} = this.props

    e.stopPropagation()
    this.props.onChangeValue(tagKey, value, !selected)
  }

  private get listItemClasses() {
    const baseClasses = 'flux-schema--item query-builder--list-item'
    return this.props.selected ? baseClasses + ' active' : baseClasses
  }
}

export default FilterTagValueListItem
