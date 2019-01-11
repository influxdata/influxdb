// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import TableSidebarItem from 'src/shared/components/tables/TableSidebarItem'

// Types
import {FluxTable} from 'src/types'

interface Props {
  data: FluxTable[]
  selectedTableName: string
  onSelectTable: (name: string) => void
}

interface State {
  searchTerm: string
}

@ErrorHandling
export default class TableSidebar extends PureComponent<Props, State> {
  public state = {
    searchTerm: '',
  }

  public render() {
    const {selectedTableName, onSelectTable} = this.props
    const {searchTerm} = this.state

    return (
      <div className="time-machine-sidebar">
        {!this.isDataEmpty && (
          <div className="time-machine-sidebar--heading">
            <input
              type="text"
              className="form-control input-xs time-machine-sidebar--filter"
              onChange={this.handleSearch}
              placeholder="Filter tables"
              value={searchTerm}
            />
          </div>
        )}
        <FancyScrollbar>
          <div className="time-machine-sidebar--items">
            {this.filteredData.map(({groupKey, id, name}) => {
              return (
                <TableSidebarItem
                  id={id}
                  key={id}
                  name={name}
                  groupKey={groupKey}
                  onSelect={onSelectTable}
                  isSelected={name === selectedTableName}
                />
              )
            })}
          </div>
        </FancyScrollbar>
      </div>
    )
  }

  private handleSearch = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  get filteredData(): FluxTable[] {
    const {data} = this.props
    const {searchTerm} = this.state

    return data.filter(d => d.name.includes(searchTerm))
  }

  get isDataEmpty(): boolean {
    return _.isEmpty(this.props.data)
  }
}
