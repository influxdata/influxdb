import React, {PureComponent} from 'react'
import {Filter} from 'src/types/logs'
import FilterBlock from 'src/logs/components/logs_filter_bar/LogsFilter'
import {Button, ComponentSize, Radio} from 'src/clockface'

interface Props {
  filters: Filter[]
  onDelete: (id: string) => void
  onClearFilters: () => void
  onFilterChange: (id: string, operator: string, value: string) => void
  onUpdateTruncation: (isTruncated: boolean) => void
  isTruncated: boolean
}

class LogsFilters extends PureComponent<Props> {
  public render() {
    return (
      <div className="logs-viewer--filter-bar">
        <div className="logs-viewer--filters">{this.renderFilters}</div>
        {this.clearFiltersButton}
        {this.truncationToggle}
      </div>
    )
  }

  private get clearFiltersButton(): JSX.Element {
    const {filters, onClearFilters} = this.props

    if (filters.length >= 3) {
      return (
        <div className="logs-viewer--clear-filters">
          <Button
            size={ComponentSize.Small}
            text="Clear Filters"
            onClick={onClearFilters}
          />
        </div>
      )
    }
  }

  private get truncationToggle(): JSX.Element {
    const {isTruncated, onUpdateTruncation} = this.props

    return (
      <Radio>
        <Radio.Button
          id="logs-truncation--truncate"
          active={isTruncated === true}
          value={true}
          titleText="Truncate log messages when they exceed 1 line"
          onClick={onUpdateTruncation}
        >
          Truncate
        </Radio.Button>
        <Radio.Button
          id="logs-truncation--multi"
          active={isTruncated === false}
          value={false}
          titleText="Allow log messages to wrap text"
          onClick={onUpdateTruncation}
        >
          Wrap
        </Radio.Button>
      </Radio>
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
