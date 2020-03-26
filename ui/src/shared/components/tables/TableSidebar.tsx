// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'
import classnames from 'classnames'

// Components
import {Input, DapperScrollbars} from '@influxdata/clockface'
import TableSidebarItem from 'src/shared/components/tables/TableSidebarItem'

// Types
import {IconFont} from '@influxdata/clockface'
import {FluxTable, Theme} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  data: FluxTable[]
  selectedTableName: string
  onSelectTable: (name: string) => void
  theme?: Theme
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
    const {selectedTableName, onSelectTable, theme} = this.props
    const {searchTerm} = this.state

    const sidebarClassName = classnames('time-machine-sidebar', {
      'time-machine-sidebar__light': theme === 'light',
    })

    return (
      <div className={sidebarClassName}>
        {!this.isDataEmpty && (
          <div className="time-machine-sidebar--heading">
            <Input
              icon={IconFont.Search}
              onChange={this.handleSearch}
              placeholder="Filter tables..."
              value={searchTerm}
              className="time-machine-sidebar--filter"
            />
          </div>
        )}
        <DapperScrollbars
          autoHide={true}
          className="time-machine-sidebar--scroll"
        >
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
        </DapperScrollbars>
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
