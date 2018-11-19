// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList} from 'src/clockface'

// Types
import {Sort} from 'src/clockface'
import {IndexHeaderCellProps} from 'src/clockface/components/index_views/IndexListHeaderCell'

type Column = IndexHeaderCellProps

interface Props {
  columns: Column[]
  onClickColumn: (nextSort: Sort, sortKey: string) => void
}

export default class DashboardIndexTableHeader extends PureComponent<Props> {
  public render() {
    const {columns, onClickColumn} = this.props
    return (
      <IndexList.Header>
        {columns.map(c => (
          <IndexList.HeaderCell
            {...c}
            key={c.columnName}
            onClick={onClickColumn}
          />
        ))}
      </IndexList.Header>
    )
  }
}
