import {proxy} from 'utils/queryUrlGenerator'

export const getAlerts = (source, timeRange, limit) =>
  proxy({
    source,
    query: `SELECT host, value, level, alertName FROM alerts WHERE time >= '${timeRange.lower}' AND time <= '${timeRange.upper}' ORDER BY time desc ${limit
      ? `LIMIT ${limit}`
      : ''}`,
    db: 'chronograf',
  })
