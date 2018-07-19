import _ from 'lodash'
import moment from 'moment'
import {map} from 'fast.js'

export const formatDate = timestamp =>
  moment(timestamp).format('M/D/YYYY h:mm:ss.SSSSSSSSS A')

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
