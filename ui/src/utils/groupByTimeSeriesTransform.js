import _ from 'lodash'
import {shiftDate} from 'shared/query/helpers'
import {map, reduce, forEach, concat, clone} from 'fast.js'

const flattenGroupBySeries = (results, responseIndex, tags) => {
  if (_.isEmpty(results)) {
    return []
  }

  const tagsKeys = _.keys(tags)

  const seriesArray = _.get(results, [0, 'series'])
  let valuesAcc
  valuesAcc = []

  seriesArray.forEach(s => {
    const tagsToAdd = tagsKeys.map(tk => s.tags[tk])
    const newValues = s.values.map(v => [v[0], ...tagsToAdd, ...v.slice(1)])
    valuesAcc = [...valuesAcc, ...newValues]
  })
  const firstColumns = _.get(results, [0, 'series', 0, 'columns'])

  const flattenedSeries = [
    {
      series: [
        {
          columns: [firstColumns[0], ...tagsKeys, ...firstColumns.slice(1)],
          tagsKeys,
          isGroupBy: true,
          tags: _.get(results, [0, 'series', 0, 'tags'], {}),
          name: _.get(results, [0, 'series', 0, 'name'], ''),
          values: [...valuesAcc],
        },
      ],
      responseIndex,
    },
  ]

  return flattenedSeries
}

const constructResults = (raw, isTable) => {
  return _.flatten(
    map(raw, (response, index) => {
      const results = _.get(response, 'response.results', [])

      const successfulResults = _.filter(results, r => _.isNil(r.error))

      const tagsFromResults = _.get(results, ['0', 'series', '0', 'tags'], {})

      if (isTable && !_.isEmpty(tagsFromResults)) {
        return flattenGroupBySeries(successfulResults, index, tagsFromResults)
      }
      return map(successfulResults, r => ({
        ...r,
        responseIndex: index,
        isGroupBy: false,
      }))
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
    isGroupBy: [],
    seriesIndex: [],
    responseIndex: [],
  }
  forEach(
    serieses,
    (
      {
        name: measurement,
        columns,
        tagsKeys,
        values = [],
        seriesIndex,
        responseIndex,
        isGroupBy,
        tags = {},
      },
      ind
    ) => {
      const rows = map(values, vals => ({vals}))

      const tagSet = map(Object.keys(tags), tag => `[${tag}=${tags[tag]}]`)
        .sort()
        .join('')

      const unsortedLabels = map(columns.slice(1), (field, i) => ({
        label:
          tagsKeys && i <= tagsKeys.length - 1
            ? `${field}`
            : `${measurement}.${field}${tagSet}`,
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
          cells.isGroupBy[cellIndex] = isGroupBy
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
  seriesLabels,
  labelsToValueIndex,
  sortedLabels
) => {
  const dashArray = Array(sortedLabels.length).fill('-')
  const timeSeries = []
  forEach(serieses, (s, sind) => {
    if (s.isGroupBy) {
      forEach(s.values, vs => {
        const tsRow = {time: vs[0], values: clone(dashArray)}
        forEach(vs.slice(1), (v, i) => {
          const label = seriesLabels[sind][i].label
          tsRow.values[
            labelsToValueIndex[label + s.responseIndex + s.seriesIndex]
          ] = v
        })
        timeSeries.push(tsRow)
      })
    }
  })

  return timeSeries
}

const constructTimeSeries = (serieses, cells, sortedLabels, seriesLabels) => {
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

    if (cells.isGroupBy[i]) {
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

export const groupByTimeSeriesTransform = (raw, isTable) => {
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
