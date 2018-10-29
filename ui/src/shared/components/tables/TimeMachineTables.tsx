// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'

// Components
import TableSidebar from 'src/flux/components/TableSidebar'
import {FluxTable} from 'src/types'
import NoResults from 'src/flux/components/NoResults'
import TableGraph from 'src/shared/components/tables/TableGraph'
import TableGraphTransform from 'src/shared/components/tables/TableGraphTransform'

// Types
import {TableView} from 'src/types/v2/dashboards'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  tables: FluxTable[]
  properties: TableView
}

interface State {
  selectedResultID: string | null
}

const filterTables = (tables: FluxTable[]): FluxTable[] => {
  const IGNORED_COLUMNS = ['', 'result', 'table', '_start', '_stop']

  return tables.map(table => {
    const header = table.data[0]
    const indices = IGNORED_COLUMNS.map(name => header.indexOf(name))
    const tableData = table.data
    const data = tableData.map(row => {
      return row.filter((__, i) => !indices.includes(i))
    })

    return {
      ...table,
      data,
    }
  })
}

const filteredTablesMemoized = memoizeOne(filterTables)

@ErrorHandling
class TimeMachineTables extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      selectedResultID: this.defaultResultId,
    }
  }

  public componentDidUpdate() {
    if (!this.selectedResult) {
      this.setState({selectedResultID: this.defaultResultId})
    }
  }

  public render() {
    const {tables, properties} = this.props

    return (
      <div className="time-machine-tables">
        {this.showSidebar && (
          <TableSidebar
            data={tables}
            selectedResultID={this.state.selectedResultID}
            onSelectResult={this.handleSelectResult}
          />
        )}
        {this.shouldShowTable && (
          <TableGraphTransform table={this.selectedResult}>
            {({data, sortedLabels}) => (
              <TableGraph
                data={data}
                sortedLabels={sortedLabels}
                properties={properties}
              />
            )}
          </TableGraphTransform>
        )}
        {!this.hasResults && <NoResults />}
      </div>
    )
  }

  private handleSelectResult = (selectedResultID: string): void => {
    this.setState({selectedResultID})
  }

  private get showSidebar(): boolean {
    return this.props.tables.length > 1
  }

  private get hasResults(): boolean {
    return !!this.props.tables.length
  }

  private get shouldShowTable(): boolean {
    return !!this.props.tables && !!this.selectedResult
  }

  private get defaultResultId() {
    const {tables} = this.props

    if (tables.length && !!tables[0]) {
      return tables[0].name
    }

    return null
  }

  private get selectedResult(): FluxTable {
    const filteredTables = filteredTablesMemoized(this.props.tables)
    const selectedResult = filteredTables.find(
      d => d.name === this.state.selectedResultID
    )

    return selectedResult
  }
}

export default TimeMachineTables
