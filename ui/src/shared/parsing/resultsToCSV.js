import _ from 'lodash'
import moment from 'moment'
import {map} from 'fast.js'

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

export const dataToCSV = ([titleRow, ...valueRows]) => {
  if (_.isEmpty(titleRow)) {
    return ''
  }
  if (_.isEmpty(valueRows)) {
    return ['date', titleRow.slice(1)].join(',')
  }
  if (titleRow[0] === 'time') {
    const titlesString = ['date', titleRow.slice(1)].join(',')

    const valuesString = map(valueRows, ([timestamp, ...values]) => [
      [formatDate(timestamp), ...values].join(','),
    ]).join('\n')
    return `${titlesString}\n${valuesString}`
  }
  const allRows = [titleRow, ...valueRows]
  const allRowsStringArray = map(allRows, r => r.join(','))
  return allRowsStringArray.join('\n')
}
