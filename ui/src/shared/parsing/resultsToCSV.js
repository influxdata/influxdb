import _ from 'lodash'
import moment from 'moment'

const resultsToCSV = results => {
  const {name, columns, values} = _.get(results, ['0', 'series', '0'], {})
  const [, ...cols] = columns

  const CSVString = [['date', ...cols].join(',')]
    .concat(
      values.map(([timestamp, ...measurements]) =>
        // MS Excel format
        [moment(timestamp).format('M/D/YYYY h:mm:ss A'), ...measurements].join(
          ','
        )
      )
    )
    .join('\n')

  return {name, CSVString}
}

export default resultsToCSV
