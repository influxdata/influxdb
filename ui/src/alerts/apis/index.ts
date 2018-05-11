import {proxy} from 'src/utils/queryUrlGenerator'
import {TimeRange} from '../../types'

export const getAlerts = (
  source: string,
  timeRange: TimeRange,
  limit: number
) => {
  const query = `SELECT host, value, level, alertName FROM alerts WHERE time >= '${
    timeRange.lower
  }' AND time <= '${timeRange.upper}' ORDER BY time desc ${
    limit ? `LIMIT ${limit}` : ''
  }`
  return proxy({
    source,
    query,
    db: 'chronograf',
  })
}
