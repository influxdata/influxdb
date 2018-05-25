import React, {PureComponent} from 'react'
import {Filter} from 'src/logs/containers/LogsPage'

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
      <li className="logs-viewer--filter">
        <span>
          {filter.key}
          {filter.operator}
          {filter.value}
        </span>
        <button
          className="logs-viewer--filter-remove"
          onClick={this.handleDeleteFilter(filter.id)}
        />
      </li>
    ))
  }

  private handleDeleteFilter = (id: string) => (): void => {
    const {filters, onUpdateFilters} = this.props

    const filteredFilters = filters.map(
      filter => (filter.id === id ? null : filter)
    )

    onUpdateFilters(filteredFilters)
  }

  private handleToggleFilter = (id: string) => (): void => {
    const {filters, onUpdateFilters} = this.props

    const filteredFilters = filters.map(filter => {
      if (filter.id === id) {
        return {...filter, enabled: !filter.enabled}
      }

      return filter
    })

    onUpdateFilters(filteredFilters)
  }
}

export default LogsFilters
