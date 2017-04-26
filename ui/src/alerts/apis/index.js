import {proxy} from 'utils/queryUrlGenerator'

export function getAlerts(source, timeRange) {
  return proxy({
    source,
    query: `SELECT host, value, level, alertName FROM alerts WHERE time >= '${timeRange.lower}' AND time <= '${timeRange.upper}' ORDER BY time desc`,
    db: 'chronograf',
  })
}
