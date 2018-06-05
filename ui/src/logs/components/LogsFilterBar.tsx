import React, {PureComponent} from 'react'
import {Filter} from 'src/logs/containers/LogsPage'
import FilterBlock from 'src/logs/components/LogsFilter'

interface Props {
  numResults: number
  filters: Filter[]
  onUpdateFilters: (fitlers: Filter[]) => void
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
        onDelete={this.handleDeleteFilter}
        onChangeOperator={this.handleChangeFilterOperator}
        onChangeValue={this.handleChangeFilterValue}
      />
    ))
  }

  private handleDeleteFilter = (id: string) => (): void => {
    const {filters, onUpdateFilters} = this.props

    const filteredFilters = filters.filter(filter => filter.id !== id)

    onUpdateFilters(filteredFilters)
  }

  private handleChangeFilterOperator = (
    id: string,
    operator: string
  ) => (): void => {
    const {filters, onUpdateFilters} = this.props

    const filteredFilters = filters.map(filter => {
      if (filter.id === id) {
        return {...filter, operator}
      }

      return filter
    })

    onUpdateFilters(filteredFilters)
  }

  private handleChangeFilterValue = (id: string, value: string) => (): void => {
    const {filters, onUpdateFilters} = this.props

    const filteredFilters = filters.map(filter => {
      if (filter.id === id) {
        return {...filter, value}
      }

      return filter
    })

    onUpdateFilters(filteredFilters)
  }
}

export default LogsFilters
