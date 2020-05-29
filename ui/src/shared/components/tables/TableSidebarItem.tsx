import React, {Fragment, PureComponent} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface GroupKey {
  [x: string]: string
}

interface Props {
  name: string
  id: string
  isSelected: boolean
  groupKey: GroupKey
  onSelect: (name: string) => void
}

@ErrorHandling
export default class TableSidebarItem extends PureComponent<Props> {
  public render() {
    const {isSelected} = this.props
    return (
      <div
        className={`time-machine-sidebar-item ${isSelected ? 'active' : ''}`}
        onClick={this.handleClick}
      >
        {this.name}
      </div>
    )
  }

  private get name(): JSX.Element[] {
    const keysIHate = ['_start', '_stop']
    return Object.entries(this.props.groupKey)
      .filter(([k]) => !keysIHate.includes(k))
      .map(([k, v], i) => {
        return (
          <Fragment key={i}>
            <span className="key">{k}</span>
            <span className="equals">=</span>
            <span className="value">{v}</span>
          </Fragment>
        )
      })
  }

  private handleClick = (): void => {
    this.props.onSelect(this.props.name)
  }
}
