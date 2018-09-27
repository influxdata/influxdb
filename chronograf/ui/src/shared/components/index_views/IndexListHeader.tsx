// Libraries
import React, {Component, CSSProperties} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

// Types
import {Alignment} from 'src/clockface'
import {IndexListColumn} from 'src/shared/components/index_views/IndexListTypes'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  columns: IndexListColumn[]
  getColumnWidthPercent: (columnKey: string) => CSSProperties
}

@ErrorHandling
class IndexListHeader extends Component<Props> {
  public render() {
    const {columns, getColumnWidthPercent} = this.props
    return (
      <thead className="index-list--header">
        {columns.map(column => (
          <th
            key={column.key}
            style={getColumnWidthPercent(column.key)}
            className={this.className(column.key)}
          >
            {column.title}
          </th>
        ))}
      </thead>
    )
  }

  private className = (columnKey: string): string => {
    const {columns} = this.props
    const {align} = _.find(columns, col => col.key === columnKey)

    return classnames('index-list--header-cell', {
      'index-list--align-left': align === Alignment.Left,
      'index-list--align-center': align === Alignment.Center,
      'index-list--align-right': align === Alignment.Right,
    })
  }
}

export default IndexListHeader
