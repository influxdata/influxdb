import React, {PureComponent} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  name: string
  id: string
  isSelected: boolean
  onSelect: (id: string) => void
}

@ErrorHandling
export default class TableSidebarItem extends PureComponent<Props> {
  public render() {
    return (
      <div
        className={`query-builder--list-item ${this.active}`}
        onClick={this.handleClick}
      >
        {this.props.name}
      </div>
    )
  }

  private get active(): string {
    if (this.props.isSelected) {
      return 'active'
    }

    return ''
  }

  private handleClick = (): void => {
    this.props.onSelect(this.props.id)
  }
}
