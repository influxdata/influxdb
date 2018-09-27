// Libraries
import React, {Component, CSSProperties} from 'react'
import _ from 'lodash'
import classnames from 'classnames'

// Components
import Header from 'src/shared/components/index_views/IndexListHeader'
import Row from 'src/shared/components/index_views/IndexListRow'

// Types
import {Alignment} from 'src/clockface'
import {
  IndexListColumn,
  IndexListRowColumn,
  IndexListRow,
} from 'src/shared/components/index_views/IndexListTypes'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  columns: IndexListColumn[]
  rows: IndexListRow[]
  emptyState: JSX.Element
}

@ErrorHandling
class IndexList extends Component<Props> {
  public render() {
    const {columns} = this.props

    return (
      <table className="index-list">
        <Header
          columns={columns}
          getColumnWidthPercent={this.getColumnWidthPercent}
        />
        {this.listRows}
      </table>
    )
  }

  private get listRows(): JSX.Element {
    const {rows, columns, emptyState} = this.props

    if (rows.length) {
      return (
        <tbody className="index-list--body">
          {rows.map((row, i) => this.listRow(row.columns, i))}
        </tbody>
      )
    }

    return (
      <tbody className="index-list--empty">
        <tr className="index-list--empty-row">
          <td colSpan={columns.length}>
            <div className="index-list--empty-cell">{emptyState}</div>
          </td>
        </tr>
      </tbody>
    )
  }

  private listRow = (
    rowColumns: IndexListRowColumn[],
    rowIndex: number
  ): JSX.Element => {
    return (
      <Row
        rowIndex={rowIndex}
        rowColumns={rowColumns}
        getColumnWidthPercent={this.getColumnWidthPercent}
        getRowColumnClassName={this.getRowColumnClassName}
      />
    )
  }

  private getRowColumnClassName = (columnKey: string): string => {
    const {columns} = this.props
    const {showOnHover, align} = _.find(columns, col => col.key === columnKey)

    return classnames('index-list--row-cell', {
      'index-list--show-hover': showOnHover,
      'index-list--align-left': align === Alignment.Left,
      'index-list--align-center': align === Alignment.Center,
      'index-list--align-right': align === Alignment.Right,
    })
  }

  private getColumnWidthPercent = (columnKey: string): CSSProperties => {
    const {columns} = this.props
    const {size} = _.find(columns, col => col.key === columnKey)

    const totalSize = _.reduce(columns, (sum, n) => sum + n.size, 0)

    const oneHundred = 100
    const columnWidthPercent = size / totalSize * oneHundred

    return {width: `${columnWidthPercent}%`}
  }
}

export default IndexList
