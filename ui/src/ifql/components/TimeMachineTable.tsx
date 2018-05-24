import React, {PureComponent} from 'react'
import _ from 'lodash'
import {Grid, GridCellProps, AutoSizer, ColumnSizer} from 'react-virtualized'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {ScriptResult} from 'src/types'

const TIME_COLUMN_WIDTH = 170

@ErrorHandling
export default class TimeMachineTable extends PureComponent<ScriptResult> {
  public render() {
    const {data} = this.props

    return (
      <div style={{flex: '1 1 auto'}}>
        <AutoSizer>
          {({height, width}) => (
            <ColumnSizer
              width={width}
              columnMinWidth={TIME_COLUMN_WIDTH}
              columnCount={this.columnCount}
            >
              {({adjustedWidth, getColumnWidth}) => (
                <Grid
                  className="table-graph--scroll-window"
                  cellRenderer={this.cellRenderer}
                  columnCount={this.columnCount}
                  columnWidth={getColumnWidth}
                  height={height}
                  rowCount={data.length}
                  rowHeight={30}
                  width={adjustedWidth}
                />
              )}
            </ColumnSizer>
          )}
        </AutoSizer>
      </div>
    )
  }

  private get columnCount(): number {
    return _.get(this.props.data, '0', []).length
  }

  private cellRenderer = ({
    columnIndex,
    key,
    rowIndex,
    style,
  }: GridCellProps): React.ReactNode => {
    const {data} = this.props
    return (
      <div key={key} style={style} className="table-graph-cell">
        {data[rowIndex][columnIndex]}
      </div>
    )
  }
}
