import {PureComponent} from 'react'
import _ from 'lodash'

import {FluxTable} from 'src/types'
import {TimeSeriesValue} from 'src/types/v2/dashboards'

interface Props {
  table: FluxTable
  children: (values: TableGraphData) => JSX.Element
}

export interface Label {
  label: string
  seriesIndex: number
  responseIndex: number
}

export interface TableGraphData {
  data: TimeSeriesValue[][]
  sortedLabels: Label[]
}

export default class TableGraphTransform extends PureComponent<Props> {
  public render() {
    return this.props.children(this.tableGraphData)
  }

  private get tableGraphData(): TableGraphData {
    const {
      table: {data = []},
    } = this.props

    const sortedLabels = _.get(data, '0', []).map(label => ({
      label,
      seriesIndex: 0,
      responseIndex: 0,
    }))

    return {data, sortedLabels}
  }
}
