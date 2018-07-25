import _ from 'lodash'
import {shiftDate} from 'src/shared/query/helpers'
import {
  fastMap,
  fastReduce,
  fastForEach,
  fastConcat,
  fastCloneArray,
} from 'src/utils/fast'

import {
  TimeSeriesServerResponse,
  TimeSeriesResult,
  TimeSeriesSeries,
  TimeSeriesValue,
  TimeSeriesSuccessfulResult,
  TimeSeries,
} from 'src/types/series'
import {getDeep} from 'src/utils/wrappers'

interface Result {
  series: TimeSeriesSeries[]
  responseIndex: number
  isGroupBy?: boolean
}

interface Series {
  name: string
  columns: string[]
  values: TimeSeriesValue[][]
  responseIndex: number
  seriesIndex: number
  isGroupBy?: boolean
  tags?: [{[x: string]: string}]
  tagsKeys?: string[]
}

interface Cells {
  isGroupBy: boolean[]
  seriesIndex: number[]
  responseIndex: number[]
  label: string[]
  value: TimeSeriesValue[]
  time: TimeSeriesValue[]
}

interface Label {
  label: string
  seriesIndex: number
  responseIndex: number
}

const flattenGroupBySeries = (
  results: TimeSeriesSuccessfulResult[],
  responseIndex: number,
  tags: {[x: string]: string}
): Result[] => {
  if (_.isEmpty(results)) {
    return []
  }

  const tagsKeys = _.keys(tags)
  const seriesArray = getDeep<TimeSeriesSeries[]>(results, '[0].series', [])

  const accumulatedValues = fastReduce<TimeSeriesSeries, TimeSeriesValue[][]>(
    seriesArray,
    (acc, s) => {
      const tagsToAdd: string[] = tagsKeys.map(tk => s.tags[tk])
      const values = s.values
      const newValues = values.map(([first, ...rest]) => [
        first,
        ...tagsToAdd,
        ...rest,
      ])
      return [...acc, ...newValues]
    },
    []
  )
  const firstColumns = getDeep<string[]>(results, '[0].series[0]columns', [])

  const flattenedSeries: Result[] = [
    {
      series: [
        {
          columns: firstColumns,
          tags: _.get(results, [0, 'series', 0, 'tags'], {}),
          name: _.get(results, [0, 'series', 0, 'name'], ''),
          values: [...accumulatedValues],
        },
      ],
      responseIndex,
      isGroupBy: true,
    },
  ]

  return flattenedSeries
}

const constructResults = (
  raw: TimeSeriesServerResponse[],
  isTable: boolean
): Result[] => {
  const MappedResponse = fastMap<TimeSeriesServerResponse, Result[]>(
    raw,
    (response, index) => {
      const results = getDeep<TimeSeriesResult[]>(
        response,
        'response.results',
        []
      )

      const successfulResults = results.filter(
        r => 'series' in r && !('error' in r)
      ) as TimeSeriesSuccessfulResult[]

      const tagsFromResults: {[x: string]: string} = _.get(
        results,
        ['0', 'series', '0', 'tags'],
        {}
      )
      const hasGroupBy = !_.isEmpty(tagsFromResults)
      if (isTable && hasGroupBy) {
        const groupBySeries = flattenGroupBySeries(
          successfulResults,
          index,
          tagsFromResults
        )
        return groupBySeries
      }

      const noGroupBySeries = fastMap<TimeSeriesSuccessfulResult, Result>(
        successfulResults,
        r => ({
          ...r,
          responseIndex: index,
        })
      )
      return noGroupBySeries
    }
  )
  return _.flatten(MappedResponse)
}

const constructSerieses = (results: Result[]): Series[] => {
  return _.flatten(
    fastMap<Result, Series[]>(results, ({series, responseIndex, isGroupBy}) =>
      fastMap<TimeSeriesSeries, Series>(series, (s, index) => ({
        ...s,
        responseIndex,
        isGroupBy,
        seriesIndex: index,
      }))
    )
  )
}

const constructCells = (
  serieses: Series[]
): {cells: Cells; sortedLabels: Label[]; seriesLabels: Label[][]} => {
  let cellIndex = 0
  let labels: Label[] = []
  const seriesLabels: Label[][] = []
  const cells: Cells = {
    label: [],
    value: [],
    time: [],
    isGroupBy: [],
    seriesIndex: [],
    responseIndex: [],
  }

  fastForEach<Series>(
    serieses,
    (
      {
        name: measurement,
        columns,
        values = [],
        seriesIndex,
        responseIndex,
        isGroupBy,
        tags = {},
      },
      ind
    ) => {
      let unsortedLabels: Label[]
      if (isGroupBy) {
        const labelsFromTags = fastMap<string, Label>(_.keys(tags), field => ({
          label: `${field}`,
          responseIndex,
          seriesIndex,
        }))
        const labelsFromColumns = fastMap<string, Label>(
          columns.slice(1),
          field => ({
            label: `${measurement}.${field}`,
            responseIndex,
            seriesIndex,
          })
        )

        unsortedLabels = fastConcat<Label>(labelsFromTags, labelsFromColumns)
        seriesLabels[ind] = unsortedLabels
        labels = _.concat(labels, unsortedLabels)
      } else {
        const tagSet = fastMap<string, string>(
          _.keys(tags),
          tag => `[${tag}=${tags[tag]}]`
        )
          .sort()
          .join('')
        unsortedLabels = fastMap<string, Label>(columns.slice(1), field => ({
          label: `${measurement}.${field}${tagSet}`,
          responseIndex,
          seriesIndex,
        }))
        seriesLabels[ind] = unsortedLabels
        labels = _.concat(labels, unsortedLabels)

        fastForEach(values, vals => {
          const [time, ...rowValues] = vals
          fastForEach(rowValues, (value, i) => {
            cells.label[cellIndex] = unsortedLabels[i].label
            cells.value[cellIndex] = value
            cells.time[cellIndex] = time
            cells.seriesIndex[cellIndex] = seriesIndex
            cells.responseIndex[cellIndex] = responseIndex
            cellIndex++ // eslint-disable-line no-plusplus
          })
        })
      }
    }
  )
  const sortedLabels = _.sortBy(labels, 'label')
  return {cells, sortedLabels, seriesLabels}
}

const insertGroupByValues = (
  serieses: Series[],
  seriesLabels: Label[][],
  labelsToValueIndex: {[x: string]: number},
  sortedLabels: Label[]
): TimeSeries[] => {
  const dashArray: TimeSeriesValue[] = Array(sortedLabels.length).fill('-')
  const timeSeries: TimeSeries[] = []

  for (let x = 0; x < serieses.length; x++) {
    const s = serieses[x]
    if (!s.isGroupBy) {
      continue
    }

    for (let i = 0; i < s.values.length; i++) {
      const [time, ...vss] = s.values[i]
      const tsRow: TimeSeries = {
        time,
        values: fastCloneArray(dashArray),
      }

      for (let j = 0; j < vss.length; j++) {
        const v = vss[j]
        const label = seriesLabels[x][j].label

        tsRow.values[
          labelsToValueIndex[label + s.responseIndex + s.seriesIndex]
        ] = v
      }

      timeSeries.push(tsRow)
    }
  }
  return timeSeries
}

const constructTimeSeries = (
  serieses: Series[],
  cells: Cells,
  sortedLabels: Label[],
  seriesLabels: Label[][]
): TimeSeries[] => {
  const nullArray: TimeSeriesValue[] = Array(sortedLabels.length).fill(null)

  const labelsToValueIndex = fastReduce<Label, {[x: string]: number}>(
    sortedLabels,
    (acc, {label, responseIndex, seriesIndex}, i) => {
      // adding series index prevents overwriting of two distinct labels that have the same field and measurements
      acc[label + responseIndex + seriesIndex] = i
      return acc
    },
    {}
  )

  const tsMemo = {}

  const timeSeries = insertGroupByValues(
    serieses,
    seriesLabels,
    labelsToValueIndex,
    sortedLabels
  )

  let existingRowIndex

  for (let i = 0; i < _.get(cells, ['value', 'length'], 0); i++) {
    let time
    time = cells.time[i]
    const value = cells.value[i]
    const label = cells.label[i]
    const seriesIndex = cells.seriesIndex[i]
    const responseIndex = cells.responseIndex[i]

    if (label.includes('_shifted__')) {
      const [, quantity, duration] = label.split('__')
      time = +shiftDate(time, quantity, duration).format('x')
    }

    existingRowIndex = tsMemo[time]

    if (existingRowIndex === undefined) {
      timeSeries.push({
        time,
        values: fastCloneArray(nullArray),
      })

      existingRowIndex = timeSeries.length - 1
      tsMemo[time] = existingRowIndex
    }

    timeSeries[existingRowIndex].values[
      labelsToValueIndex[label + responseIndex + seriesIndex]
    ] = value
  }

  return _.sortBy(timeSeries, 'time')
}

export const groupByTimeSeriesTransform = (
  raw: TimeSeriesServerResponse[],
  isTable: boolean
): {sortedLabels: Label[]; sortedTimeSeries: TimeSeries[]} => {
  const results = constructResults(raw, isTable)
  const serieses = constructSerieses(results)
  const {cells, sortedLabels, seriesLabels} = constructCells(serieses)
  const sortedTimeSeries = constructTimeSeries(
    serieses,
    cells,
    sortedLabels,
    seriesLabels
  )
  return {
    sortedLabels,
    sortedTimeSeries,
  }
}
