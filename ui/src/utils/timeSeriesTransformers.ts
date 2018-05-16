import {fastMap, fastReduce} from 'src/utils/fast'
import {groupByTimeSeriesTransform} from 'src/utils/groupByTimeSeriesTransform'

import {TimeSeriesServerResponse, TimeSeries} from 'src/types/series'
import {TimeSeriesValue} from 'src/types/series'

type DygraphValue = string | number | Date | null

interface Label {
  label: string
  seriesIndex: number
  responseIndex: number
}

interface TimeSeriesToDyGraphReturnType {
  labels: string[]
  timeSeries: DygraphValue[][]
  dygraphSeries: DygraphSeries
}

interface TimeSeriesToTableGraphReturnType {
  data: TimeSeriesValue[][]
  sortedLabels: Label[]
}

interface DygraphSeries {
  [x: string]: {
    axis: string
  }
}

export const timeSeriesToDygraph = (
  raw: TimeSeriesServerResponse[],
  isInDataExplorer: boolean
): TimeSeriesToDyGraphReturnType => {
  const isTable = false
  const {sortedLabels, sortedTimeSeries} = groupByTimeSeriesTransform(
    raw,
    isTable
  )

  const labels = [
    'time',
    ...fastMap<Label, string>(sortedLabels, ({label}) => label),
  ]

  const timeSeries = fastMap<TimeSeries, DygraphValue[]>(
    sortedTimeSeries,
    ({time, values}) => [new Date(time), ...values]
  )

  const dygraphSeries = fastReduce<Label, DygraphSeries>(
    sortedLabels,
    (acc, {label, responseIndex}) => {
      if (!isInDataExplorer) {
        acc[label] = {
          axis: responseIndex === 0 ? 'y' : 'y2',
        }
      }
      return acc
    },
    {}
  )

  return {labels, timeSeries, dygraphSeries}
}

export const timeSeriesToTableGraph = (
  raw: TimeSeriesServerResponse[]
): TimeSeriesToTableGraphReturnType => {
  const isTable = true
  const {sortedLabels, sortedTimeSeries} = groupByTimeSeriesTransform(
    raw,
    isTable
  )

  const labels = [
    'time',
    ...fastMap<Label, string>(sortedLabels, ({label}) => label),
  ]

  const tableData = fastMap<TimeSeries, TimeSeriesValue[]>(
    sortedTimeSeries,
    ({time, values}) => [time, ...values]
  )
  const data = tableData.length ? [labels, ...tableData] : [[]]
  return {
    data,
    sortedLabels,
  }
}
