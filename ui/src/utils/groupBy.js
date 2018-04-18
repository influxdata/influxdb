import _ from 'lodash'
import {shiftDate} from 'shared/query/helpers'
import {map, reduce, forEach, concat, clone} from 'fast.js'

const groupByTransform = (responses, responseIndex, groupBys) => {
  const firstColumns = _.get(responses, [0, 'series', 0, 'columns'])
  const accum = [
    {
      responseIndex,
      series: [
        {
          columns: [firstColumns[0], ...groupBys, ...firstColumns.slice(1)],
          name: _.get(responses, [0, 'series', 0, 'name']),
          values: [],
        },
      ],
    },
  ]

  const seriesArray = _.get(responses, [0, 'series'])
  seriesArray.forEach(s => {
    const prevValues = accum[0].series[0].values
    const tagsToAdd = groupBys.map(gb => s.tags[gb])
    const newValues = s.values.map(v => [v[0], ...tagsToAdd, ...v.slice(1)])
    accum[0].series[0].values = [...prevValues, ...newValues]
  })
  return accum
}

export const groupByTimeSeriesTransform = (raw = [], queryASTs = []) => {
  const groupBys = queryASTs.map(queryAST => {
    return _.get(queryAST, ['groupBy', 'tags'], false)
  })
  const results = reduce(
    raw,
    (acc, rawResponse, responseIndex) => {
      const responses = _.get(rawResponse, 'response.results', [])

      const indexedResponses = groupBys[responseIndex]
        ? groupByTransform(responses, responseIndex, groupBys[responseIndex])
        : map(responses, response => ({
            ...response,
            responseIndex,
          }))

      return [...acc, ...indexedResponses]
    },
    []
  )
  const serieses = reduce(
    results,
    (acc, {series = [], responseIndex}) => {
      return [
        ...acc,
        ...map(series, (item, index) => ({
          ...item,
          responseIndex,
          seriesIndex: index,
        })),
      ]
    },
    []
  )
  const size = reduce(
    serieses,
    (acc, {columns, values}) => {
      if (columns.length && (values && values.length)) {
        return acc + (columns.length - 1) * values.length
      }
      return acc
    },
    0
  )
  // console.log('size', size)
  // convert series into cells with rows and columns
  let cellIndex = 0
  let labels = []
  const DEFAULT_SIZE = 0
  const cells = {
    label: new Array(DEFAULT_SIZE),
    value: new Array(DEFAULT_SIZE),
    time: new Array(DEFAULT_SIZE),
    groupByVals: new Array(DEFAULT_SIZE),
    groupByLabels: new Array(DEFAULT_SIZE),
    seriesIndex: new Array(DEFAULT_SIZE),
    responseIndex: new Array(DEFAULT_SIZE),
  }

  forEach(
    serieses,
    (
      {
        name: measurement,
        columns,
        values,
        seriesIndex,
        responseIndex,
        tags = {},
      },
      ind
    ) => {
      const rows = map(values || [], vals => ({
        vals,
      }))

      const unsortedLabels = map(columns.slice(1), field => ({
        label: `${measurement}.${field}`,
        responseIndex,
        seriesIndex,
      }))
      serieses[ind].unsortedLabels = unsortedLabels
      labels = concat(labels, unsortedLabels)
      const groupByTags = groupBys[responseIndex]
      cells.groupByLabels = groupByTags

      forEach(rows, ({vals}) => {
        const [time, ...rowValues] = vals
        forEach(rowValues, (value, i) => {
          cells.label[cellIndex] = unsortedLabels[i].label
          cells.value[cellIndex] = value
          cells.time[cellIndex] = time
          if (!_.isEmpty(groupByTags)) {
            cells.groupByVals[cellIndex] = groupByTags.map(gb => tags[gb])
          }
          cells.seriesIndex[cellIndex] = seriesIndex
          cells.responseIndex[cellIndex] = responseIndex
          cellIndex++ // eslint-disable-line no-plusplus
        })
      })
    }
  )

  const sortedLabels = _.sortBy(labels, 'label')
  const tsMemo = {}
  const nullArray = Array(sortedLabels.length).fill('null')
  const slashArray = Array(sortedLabels.length).fill('-')

  const labelsToValueIndex = reduce(
    sortedLabels,
    (acc, {label, seriesIndex}, i) => {
      // adding series index prevents overwriting of two distinct labels that have the same field and measurements
      acc[label + seriesIndex] = i
      return acc
    },
    {}
  )

  const timeSeries = []
  let existingRowIndex
  forEach(serieses, s => {
    if (groupBys[s.responseIndex]) {
      forEach(s.values, vs => {
        timeSeries.push({time: vs[0], values: clone(slashArray)})
        existingRowIndex = timeSeries.length - 1
        forEach(vs.slice(1), (v, i) => {
          const label = s.unsortedLabels[i].label
          timeSeries[existingRowIndex].values[
            labelsToValueIndex[label + s.seriesIndex]
          ] = v
        })
      })
    }
  })
  for (let i = 0; i < size; i++) {
    let time
    time = cells.time[i]
    const value = cells.value[i]
    const label = cells.label[i]
    const seriesIndex = cells.seriesIndex[i]

    if (!groupBys[cells.responseIndex[i]]) {
      if (label.includes('_shifted__')) {
        const [, quantity, duration] = label.split('__')
        time = +shiftDate(time, quantity, duration).format('x')
      }

      existingRowIndex = tsMemo[time]

      if (existingRowIndex === undefined) {
        timeSeries.push({
          time,
          values: clone(slashArray),
        })

        existingRowIndex = timeSeries.length - 1
        tsMemo[time] = existingRowIndex
      }

      timeSeries[existingRowIndex].values[
        labelsToValueIndex[label + seriesIndex]
      ] = value
    }
  }

  const sortedTimeSeries = _.sortBy(timeSeries, 'time')

  return {
    sortedLabels,
    sortedTimeSeries,
  }
}
