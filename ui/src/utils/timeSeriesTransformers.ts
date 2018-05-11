import {map, reduce} from 'fast.js'
import {groupByTimeSeriesTransform} from 'src/utils/groupByTimeSeriesTransform'

import {TimeSeriesServerResponse} from 'src/types/series'
import {DbData} from '../types/dashboard'

interface Label {
  label: string
  seriesIndex: number
  responseIndex: number
}

interface TimeSeries {
  time: DbData[]
  values: DbData[]
}

interface TimeSeriesToDyGraphReturnType {
  labels: string[]
  timeSeries: TimeSeries[]
  dygraphSeries: DygraphSeries
}

interface TimeSeriesToTableGraphReturnType {
  data: DbData[][]
  sortedLabels: Label[]
}

interface DygraphSeries {
  [x: string]: {
    axis: string
  }
}

export const timeSeriesToDygraph = (
  raw: TimeSeriesServerResponse,
  isInDataExplorer: boolean
): TimeSeriesToDyGraphReturnType => {
  const isTable = false
  const {sortedLabels, sortedTimeSeries} = groupByTimeSeriesTransform(
    raw,
    isTable
  )

  const dygraphSeries = reduce(
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
  return {
    labels: ['time', ...map(sortedLabels, ({label}) => label)],
    timeSeries: map(sortedTimeSeries, ({time, values}) => [
      new Date(time),
      ...values,
    ]),
    dygraphSeries,
  }
}

export const timeSeriesToTableGraph = (
  raw: TimeSeriesServerResponse
): TimeSeriesToTableGraphReturnType => {
  const isTable = true
  const {sortedLabels, sortedTimeSeries} = groupByTimeSeriesTransform(
    raw,
    isTable
  )

  const labels = ['time', ...map(sortedLabels, ({label}) => label)]

  const tableData = map(sortedTimeSeries, ({time, values}) => [time, ...values])
  const data = tableData.length ? [labels, ...tableData] : [[]]
  return {
    data,
    sortedLabels,
  }
}
