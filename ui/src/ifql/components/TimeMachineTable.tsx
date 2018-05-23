import React, {PureComponent} from 'react'
import {Grid, GridCellProps, AutoSizer} from 'react-virtualized'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {ScriptResult} from 'src/types'

@ErrorHandling
export default class TimeMachineTable extends PureComponent<ScriptResult> {
  public render() {
    const {data} = this.props

    return (
      <div style={{flex: '1 1 auto'}}>
        <AutoSizer>
          {({height, width}) => (
            <Grid
              className="table-graph--scroll-window"
              cellRenderer={this.cellRenderer}
              columnCount={data[0].length}
              columnWidth={100}
              height={height}
              rowCount={data.length}
              rowHeight={30}
              width={width}
            />
          )}
        </AutoSizer>
      </div>
    )
  }

  private cellRenderer = ({
    columnIndex,
    key,
    rowIndex,
    style,
  }: GridCellProps): React.ReactNode => {
    const {data} = this.props
    return (
      <div key={key} style={style}>
        {data[rowIndex][columnIndex]}
      </div>
    )
  }
}
