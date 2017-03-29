import {proxy} from 'utils/queryUrlGenerator'

export default function fetchTimeSeries(source, database, query) {
  return proxy({source, query, database})
}
