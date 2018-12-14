import React, {PureComponent} from 'react'
import _ from 'lodash'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {
  ASCENDING,
  DEFAULT_TIME_FIELD,
  DESCENDING,
  DEFAULT_SORT_DIRECTION,
} from 'src/shared/constants/tableGraph'
import {FluxTable} from 'src/types'
import {TableView, SortOptions} from 'src/types/v2/dashboards'
import TableGraphTransform from 'src/shared/components/tables/TableGraphTransform'
import TableGraphTable from 'src/shared/components/tables/TableGraphTable'

interface Props {
  table: FluxTable
  properties: TableView
}

interface State {
  sortOptions: SortOptions
}

@ErrorHandling
class TableGraph extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)
    const sortField = _.get(
      props,
      'properties.tableOptions.sortBy.internalName'
    )

    this.state = {
      sortOptions: {
        field: sortField || DEFAULT_TIME_FIELD.internalName,
        direction: ASCENDING,
      },
    }
  }

  public render() {
    const {table, properties} = this.props
    const {sortOptions} = this.state
    return (
      <TableGraphTransform
        data={table.data}
        properties={properties}
        sortOptions={sortOptions}
      >
        {transformedDataBundle => (
          <TableGraphTable
            transformedDataBundle={transformedDataBundle}
            onSort={this.handleSetSort}
            properties={properties}
          />
        )}
      </TableGraphTransform>
    )
  }

  public handleSetSort = (fieldName: string) => {
    const {sortOptions} = this.state

    if (fieldName === sortOptions.field) {
      sortOptions.direction =
        sortOptions.direction === ASCENDING ? DESCENDING : ASCENDING
    } else {
      sortOptions.field = fieldName
      sortOptions.direction = DEFAULT_SORT_DIRECTION
    }
    this.setState({sortOptions})
  }
}

export default TableGraph
