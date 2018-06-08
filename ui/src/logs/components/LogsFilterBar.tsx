import React, {PureComponent} from 'react'
import {Filter} from 'src/types/logs'
import FilterBlock from 'src/logs/components/LogsFilter'

interface Props {
  numResults: number
  filters: Filter[]
  onDelete: (id: string) => void
  onFilterChange: (id: string, operator: string, value: string) => void
}

class LogsFilters extends PureComponent<Props> {
  public render() {
    const {numResults} = this.props

    return (
      <div className="logs-viewer--filter-bar">
        <label className="logs-viewer--results-text">
          Query returned <strong>{numResults} Events</strong>
        </label>
        <ul className="logs-viewer--filters">{this.renderFilters}</ul>
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
