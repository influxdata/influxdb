// Libraries
import React, {Component, CSSProperties} from 'react'
import _ from 'lodash'

// Types
import {IndexListRowColumn} from 'src/shared/components/index_views/IndexListTypes'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  rowIndex: number
  rowColumns: IndexListRowColumn[]
  getColumnWidthPercent: (columnKey: string) => CSSProperties
  getRowColumnClassName: (columnKey: string) => string
}

@ErrorHandling
class IndexListRow extends Component<Props> {
  public render() {
    const {
      rowColumns,
      rowIndex,
      getColumnWidthPercent,
      getRowColumnClassName,
    } = this.props

    return (
      <tr className="index-list--row">
        {rowColumns.map(rowColumn => (
          <td
            key={`index-list--row-${rowIndex}-col-${rowColumn.key}`}
            className={getRowColumnClassName(rowColumn.key)}
            style={getColumnWidthPercent(rowColumn.key)}
          >
            <div className="index-list--cell">{rowColumn.contents}</div>
          </td>
        ))}
      </tr>
    )
  }
}

export default IndexListRow
