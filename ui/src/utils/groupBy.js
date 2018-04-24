import _ from 'lodash'
import {shiftDate} from 'shared/query/helpers'
import {map, reduce, forEach, concat, clone} from 'fast.js'

const groupByMap = (responses, responseIndex, groupByColumns) => {
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

const constructResults = (raw, groupBys) => {
  return _.flatten(
    map(raw, (response, index) => {
      const responses = _.get(response, 'response.results', [])

      if (groupBys[index]) {
        return groupByMap(responses, index, groupBys[index])
      }

      return map(responses, r => ({...r, responseIndex: index}))
    })
  )
}

const constructSerieses = results => {
  return reduce(
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
}

const constructCells = serieses => {
  let cellIndex = 0
  let labels = []
  const seriesLabels = []
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
      seriesLabels[ind] = unsortedLabels
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
  return {cells, sortedLabels, seriesLabels}
}

const insertGroupByValues = (
  serieses,
  groupBys,
  seriesLabels,
  labelsToValueIndex,
  sortedLabels
) => {
  const dashArray = Array(sortedLabels.length).fill('-')
  const timeSeries = []
  let existingRowIndex
  forEach(serieses, (s, sind) => {
    if (groupBys[s.responseIndex]) {
      forEach(s.values, vs => {
        timeSeries.push({time: vs[0], values: clone(dashArray)})
        existingRowIndex = timeSeries.length - 1
        forEach(vs.slice(1), (v, i) => {
          const label = seriesLabels[sind][i].label
          timeSeries[existingRowIndex].values[
            labelsToValueIndex[label + s.responseIndex + s.seriesIndex]
          ] = v
        })
      })
    }
  })

  return timeSeries
}

const constructTimeSeries = (
  serieses,
  cells,
  sortedLabels,
  groupBys,
  seriesLabels
) => {
  const nullArray = Array(sortedLabels.length).fill(null)

  const labelsToValueIndex = reduce(
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
    groupBys,
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

    if (groupBys[cells.responseIndex[i]]) {
      // we've already inserted GroupByValues
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
        values: clone(nullArray),
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

export const groupByTimeSeriesTransform = (raw, groupBys) => {
  if (!groupBys) {
    groupBys = Array(raw.length).fill(false)
  }
  const results = constructResults(raw, groupBys)

  const serieses = constructSerieses(results)

  const {cells, sortedLabels, seriesLabels} = constructCells(serieses)

  const sortedTimeSeries = constructTimeSeries(
    serieses,
    cells,
    sortedLabels,
    groupBys,
    seriesLabels
  )

  return {
    sortedLabels,
    sortedTimeSeries,
  }
}
