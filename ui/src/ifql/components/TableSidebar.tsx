import React, {PureComponent, CSSProperties} from 'react'
import _ from 'lodash'

import {ScriptResult} from 'src/types'
import {ErrorHandling} from 'src/shared/decorators/errors'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import TableSidebarItem from 'src/ifql/components/TableSidebarItem'
import {vis} from 'src/ifql/constants'

interface Props {
  data: ScriptResult[]
  selectedResultID: string
  onSelectResult: (id: string) => void
}

@ErrorHandling
export default class TableSidebar extends PureComponent<Props> {
  public render() {
    const {data, selectedResultID, onSelectResult} = this.props

    return (
      <div className="time-machine--sidebar">
        {!this.isDataEmpty && (
          <div className="query-builder--heading" style={this.headingStyle}>
            Results
          </div>
        )}
        <FancyScrollbar>
          <div className="time-machine-vis--sidebar query-builder--list">
            {data.map(({name, id}) => {
              return (
                <TableSidebarItem
                  id={id}
                  key={id}
                  name={name}
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

  get headingStyle(): CSSProperties {
    return {
      height: `${vis.TABLE_ROW_HEIGHT + 2.5}px`,
      backgroundColor: '#31313d',
      borderBottom: '2px solid #383846', // $g5-pepper
    }
  }

  get isDataEmpty(): boolean {
    return _.isEmpty(this.props.data)
  }
}
