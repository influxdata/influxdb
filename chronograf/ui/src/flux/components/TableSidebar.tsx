import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

import {FluxTable} from 'src/types'
import {ErrorHandling} from 'src/shared/decorators/errors'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import TableSidebarItem from 'src/flux/components/TableSidebarItem'

interface Props {
  data: FluxTable[]
  selectedResultID: string
  onSelectResult: (id: string) => void
}

interface State {
  searchTerm: string
}

@ErrorHandling
export default class TableSidebar extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {selectedResultID, onSelectResult} = this.props
    const {searchTerm} = this.state

    return (
      <div className="yield-node--sidebar">
        {!this.isDataEmpty && (
          <div className="yield-node--sidebar-heading">
            <input
              type="text"
              className="form-control input-xs yield-node--sidebar-filter"
              onChange={this.handleSearch}
              placeholder="Filter tables"
              value={searchTerm}
            />
          </div>
        )}
        <FancyScrollbar>
          <div className="yield-node--tabs">
            {this.data.map(({groupKey, id}) => {
              return (
                <TableSidebarItem
                  id={id}
                  key={id}
                  name={name}
                  groupKey={groupKey}
                  onSelect={onSelectResult}
                  isSelected={id === selectedResultID}
                />
              )
            })}
          </div>
        </FancyScrollbar>
      </div>
    )
  }

  private handleSearch = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({searchTerm: e.target.value})
  }

  get data(): FluxTable[] {
    const {data} = this.props
    const {searchTerm} = this.state

    return data.filter(d => d.name.includes(searchTerm))
  }

  get isDataEmpty(): boolean {
    return _.isEmpty(this.props.data)
  }
}
