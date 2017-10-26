import _ from 'lodash'
import moment from 'moment'

export const formatDate = timestamp =>
  moment(timestamp).format('M/D/YYYY h:mm:ss.SSSSSSSSS A')

export const resultsToCSV = results => {
  if (!_.get(results, ['0', 'series', '0'])) {
    return {flag: 'no_data', name: '', CSVString: ''}
  }

  const {name, columns, values} = _.get(results, ['0', 'series', '0'])

  if (columns[0] === 'time') {
    const [, ...cols] = columns
    const CSVString = [['date', ...cols].join(',')]
      .concat(
        values.map(([timestamp, ...measurements]) =>
          // MS Excel format
          [formatDate(timestamp), ...measurements].join(',')
        )
      )
      .join('\n')
    return {flag: 'ok', name, CSVString}
  }

  const CSVString = [columns.join(',')]
    .concat(values.map(row => row.join(',')))
    .join('\n')
  return {flag: 'ok', name, CSVString}
}

export const dashboardtoCSV = data => {
  const columnNames = _.flatten(
    data.map(r => _.get(r, 'results[0].series[0].columns', []))
  )
  const timeIndices = columnNames
    .map((e, i) => (e === 'time' ? i : -1))
    .filter(e => e >= 0)

  let values = data.map(r => _.get(r, 'results[0].series[0].values', []))
  values = _.unzip(values).map(v => _.flatten(v))
  if (timeIndices) {
    values.map(v => {
      timeIndices.forEach(i => (v[i] = formatDate(v[i])))
      return v
    })
  }
  const CSVString = [columnNames.join(',')]
    .concat(values.map(v => v.join(',')))
    .join('\n')
  return CSVString
}
