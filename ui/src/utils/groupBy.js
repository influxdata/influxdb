import _ from 'lodash'
import {shiftDate} from 'shared/query/helpers'
import {map, reduce, forEach, concat, clone} from 'fast.js'

const groupByTransform = (responses, responseIndex, groupByColumns) => {
  const firstColumns = _.get(responses, [0, 'series', 0, 'columns'])
  const accum = [
    {
      responseIndex,
      series: [
        {
          columns: [
            firstColumns[0],
            ...groupByColumns,
            ...firstColumns.slice(1),
          ],
          groupByColumns,
          name: _.get(responses, [0, 'series', 0, 'name']),
          values: [],
        },
      ],
    },
  ]

  const seriesArray = _.get(responses, [0, 'series'])
  seriesArray.forEach(s => {
    const prevValues = accum[0].series[0].values
    const tagsToAdd = groupByColumns.map(gb => s.tags[gb])
    const newValues = s.values.map(v => [v[0], ...tagsToAdd, ...v.slice(1)])
    accum[0].series[0].values = [...prevValues, ...newValues]
  })
  return accum
}

export const groupByTimeSeriesTransform = (raw = [], groupBys = []) => {
  const results = _.flatten(
    map(raw, (response, index) => {
      const responses = _.get(response, 'response.results', [])

      if (groupBys[index]) {
        return groupByTransform(responses, index, groupBys[index])
      }

      return map(responses, r => ({...r, responseIndex: index}))
    })
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
  // convert series into cells with rows and columns
  let cellIndex = 0
  let labels = []
  const cells = {
    label: [],
    value: [],
    time: [],
    seriesIndex: [],
    responseIndex: [],
  }

  forEach(
    serieses,
    (
      {
        name: measurement,
        columns,
        groupByColumns,
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

      const unsortedLabels = map(columns.slice(1), (field, i) => ({
        label:
          groupByColumns && i <= groupByColumns.length - 1
            ? `${field}`
            : `${measurement}.${field}`,
        responseIndex,
        seriesIndex,
      }))
      serieses[ind].unsortedLabels = unsortedLabels
      labels = concat(labels, unsortedLabels)

      forEach(rows, ({vals}) => {
        const [time, ...rowValues] = vals
        forEach(rowValues, (value, i) => {
          cells.label[cellIndex] = unsortedLabels[i].label
          cells.value[cellIndex] = value
          cells.time[cellIndex] = time
          cells.seriesIndex[cellIndex] = seriesIndex
          cells.responseIndex[cellIndex] = responseIndex
          cellIndex++ // eslint-disable-line no-plusplus
        })
      })
    }
  )

  const sortedLabels = _.sortBy(labels, 'label')
  const tsMemo = {}
  const dashArray = Array(sortedLabels.length).fill('-')

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
        timeSeries.push({time: vs[0], values: clone(dashArray)})
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

  const size = reduce(
    serieses,
    (acc, {columns, values}) => {
      return acc + _.get(columns, 'length', 0) * _.get(values, 'length', 0)
    },
    0
  )

  for (let i = 0; i < size; i++) {
    let time
    time = cells.time[i]
    const value = cells.value[i]
    const label = _.get(cells.label, i, '')
    const seriesIndex = cells.seriesIndex[i]

    if (groupBys[cells.responseIndex[i]]) {
      continue
    }

    if (label.includes('_shifted__')) {
      const [, quantity, duration] = label.split('__')
      time = +shiftDate(time, quantity, duration).format('x')
    }

    existingRowIndex = tsMemo[time]

    if (existingRowIndex === undefined) {
      timeSeries.push({
        time,
        values: clone(dashArray),
      })

      existingRowIndex = timeSeries.length - 1
      tsMemo[time] = existingRowIndex
    }

    timeSeries[existingRowIndex].values[
      labelsToValueIndex[label + seriesIndex]
    ] = value
  }

  const sortedTimeSeries = _.sortBy(timeSeries, 'time')

  return {
    sortedLabels,
    sortedTimeSeries,
  }
}
