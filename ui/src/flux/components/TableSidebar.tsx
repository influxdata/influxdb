import React, {PureComponent, CSSProperties, ChangeEvent} from 'react'
import _ from 'lodash'

import {FluxTable} from 'src/types'
import {ErrorHandling} from 'src/shared/decorators/errors'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import TableSidebarItem from 'src/flux/components/TableSidebarItem'
import {vis} from 'src/flux/constants'

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
      <div className="time-machine--sidebar">
        {!this.isDataEmpty && (
          <div
            className="time-machine-sidebar--heading"
            style={this.headingStyle}
          >
            Tables
            <div className="time-machine-sidebar--filter">
              <input
                type="text"
                className="form-control input-xs"
                onChange={this.handleSearch}
                placeholder="Filter tables"
                value={searchTerm}
              />
            </div>
          </div>
        )}
        <FancyScrollbar>
          <div className="time-machine-vis--sidebar query-builder--list">
            {this.data.map(({partitionKey, id}) => {
              return (
                <TableSidebarItem
                  id={id}
                  key={id}
                  name={name}
                  partitionKey={partitionKey}
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

  get headingStyle(): CSSProperties {
    return {
      height: `${vis.TABLE_ROW_HEADER_HEIGHT + 4}px`,
      backgroundColor: '#31313d',
      borderBottom: '2px solid #383846', // $g5-pepper
    }
  }

  get isDataEmpty(): boolean {
    return _.isEmpty(this.props.data)
  }
}
