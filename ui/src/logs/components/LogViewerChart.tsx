import React, {PureComponent} from 'react'
import LineGraph from 'src/shared/components/LineGraph'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'

import {TimeRange} from 'src/types'

interface Props {
  onZoom: (lower: string, upper: string) => void
  timeRange: TimeRange
  data: object[]
}

class LogViewerChart extends PureComponent<Props> {
  public render() {
    const {timeRange, data, onZoom} = this.props
    return (
      <LineGraph
        onZoom={onZoom}
        queries={[]}
        data={data}
        isBarGraph={true}
        timeRange={timeRange}
        displayOptions={{animatedZooms: false}}
        setResolution={this.setResolution}
        colors={DEFAULT_LINE_COLORS}
      />
    )
  }

  private setResolution = () => {}
}

export default LogViewerChart
