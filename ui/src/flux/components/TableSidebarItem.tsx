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
  onSelect: (id: string) => void
}

@ErrorHandling
export default class TableSidebarItem extends PureComponent<Props> {
  public render() {
    return (
      <div
        className={`yield-node--tab ${this.active}`}
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
