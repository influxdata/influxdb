import _ from 'lodash'

const resultsToCSV = results => {
  const name = _.get(results, ['0', 'series', '0', 'name'])
  const columns = _.get(results, ['0', 'series', '0', 'columns'])
  const values = _.get(results, ['0', 'series', '0', 'values'])

  const CSVString = `\"${_.join(columns, '","')}\"\n${values
    .map(arr => `${arr[0].toString()},${arr[1].toString()}`)
    .join('\n')}`
  return {name, CSVString}
}
export default resultsToCSV
