import _ from 'lodash'
import moment from 'moment'

export const formatDate = timestamp =>
  moment(timestamp).format('M/D/YYYY h:mm:ss A')

const resultsToCSV = results => {
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

export default resultsToCSV
