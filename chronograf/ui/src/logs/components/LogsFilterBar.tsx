import React, {PureComponent} from 'react'
import {Filter} from 'src/types/logs'
import FilterBlock from 'src/logs/components/LogsFilter'
import QueryResults from 'src/logs/components/QueryResults'

interface Props {
  numResults: number
  filters: Filter[]
  queryCount: number
  onDelete: (id: string) => void
  onFilterChange: (id: string, operator: string, value: string) => void
}

class LogsFilters extends PureComponent<Props> {
  public render() {
    const {numResults, queryCount} = this.props

    return (
      <div className="logs-viewer--filter-bar">
        <QueryResults count={numResults} queryCount={queryCount} />
        <div className="logs-viewer--filters">{this.renderFilters}</div>
      </div>
    )
  }

  private get renderFilters(): JSX.Element[] {
    const {filters} = this.props

    return filters.map(filter => (
      <FilterBlock
        key={filter.id}
        filter={filter}
        onDelete={this.props.onDelete}
        onChangeFilter={this.props.onFilterChange}
      />
    ))
  }
}

export default LogsFilters
