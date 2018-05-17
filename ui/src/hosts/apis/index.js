import {proxy} from 'utils/queryUrlGenerator'
import AJAX from 'utils/ajax'
import _ from 'lodash'

export const getCpuAndLoadForHosts = (
  proxyLink,
  telegrafDB,
  telegrafSystemInterval,
  tempVars
) => {
  return proxy({
    source: proxyLink,
    query: `SELECT mean("usage_user") FROM \":db:\".\":rp:\".\"cpu\" WHERE "cpu" = 'cpu-total' AND time > now() - 10m GROUP BY host;
      SELECT mean("load1") FROM \":db:\".\":rp:\".\"system\" WHERE time > now() - 10m GROUP BY host;
      SELECT non_negative_derivative(mean(uptime)) AS deltaUptime FROM \":db:\".\":rp:\".\"system\" WHERE time > now() - ${telegrafSystemInterval} * 10 GROUP BY host, time(${telegrafSystemInterval}) fill(0);
      SELECT mean("Percent_Processor_Time") FROM \":db:\".\":rp:\".\"win_cpu\" WHERE time > now() - 10m GROUP BY host;
      SELECT mean("Processor_Queue_Length") FROM \":db:\".\":rp:\".\"win_system\" WHERE time > now() - 10s GROUP BY host;
      SELECT non_negative_derivative(mean("System_Up_Time")) AS winDeltaUptime FROM \":db:\".\":rp:\".\"win_system\" WHERE time > now() - ${telegrafSystemInterval} * 10 GROUP BY host, time(${telegrafSystemInterval}) fill(0);
      SHOW TAG VALUES WITH KEY = "host";`,
    db: telegrafDB,
    tempVars,
  }).then(resp => {
    const hosts = {}
    const precision = 100
    const cpuSeries = _.get(resp, ['data', 'results', '0', 'series'], [])
    const loadSeries = _.get(resp, ['data', 'results', '1', 'series'], [])
    const uptimeSeries = _.get(resp, ['data', 'results', '2', 'series'], [])
    const winCPUSeries = _.get(resp, ['data', 'results', '3', 'series'], [])
    const winLoadSeries = _.get(resp, ['data', 'results', '4', 'series'], [])
    const winUptimeSeries = _.get(resp, ['data', 'results', '5', 'series'], [])
    const allHostsSeries = _.get(resp, ['data', 'results', '6', 'series'], [])

    allHostsSeries.forEach(s => {
      const hostnameIndex = s.columns.findIndex(col => col === 'value')
      s.values.forEach(v => {
        const hostname = v[hostnameIndex]
        hosts[hostname] = {
          name: hostname,
          deltaUptime: -1,
          cpu: 0.0,
          load: 0.0,
        }
      })
    })

    cpuSeries.forEach(s => {
      const meanIndex = s.columns.findIndex(col => col === 'mean')
      hosts[s.tags.host] = {
        name: s.tags.host,
        cpu: Math.round(s.values[0][meanIndex] * precision) / precision,
      }
    })

    loadSeries.forEach(s => {
      const meanIndex = s.columns.findIndex(col => col === 'mean')
      hosts[s.tags.host].load =
        Math.round(s.values[0][meanIndex] * precision) / precision
    })

    uptimeSeries.forEach(s => {
      const uptimeIndex = s.columns.findIndex(col => col === 'deltaUptime')
      hosts[s.tags.host].deltaUptime =
        s.values[s.values.length - 1][uptimeIndex]
    })

    winCPUSeries.forEach(s => {
      const meanIndex = s.columns.findIndex(col => col === 'mean')
      hosts[s.tags.host] = {
        name: s.tags.host,
        cpu: Math.round(s.values[0][meanIndex] * precision) / precision,
      }
    })

    winLoadSeries.forEach(s => {
      const meanIndex = s.columns.findIndex(col => col === 'mean')
      hosts[s.tags.host].load =
        Math.round(s.values[0][meanIndex] * precision) / precision
    })

    winUptimeSeries.forEach(s => {
      const winUptimeIndex = s.columns.findIndex(
        col => col === 'winDeltaUptime'
      )
      hosts[s.tags.host].winDeltaUptime =
        s.values[s.values.length - 1][winUptimeIndex]
    })

    return hosts
  })
}

export async function getAllHosts(proxyLink, telegrafDB) {
  try {
    const resp = await proxy({
      source: proxyLink,
      query: 'show tag values with key = "host"',
      db: telegrafDB,
    })
    const hosts = {}
    const allHostsSeries = _.get(resp, ['data', 'results', '0', 'series'], [])

    allHostsSeries.forEach(s => {
      const hostnameIndex = s.columns.findIndex(col => col === 'value')
      s.values.forEach(v => {
        const hostname = v[hostnameIndex]
        hosts[hostname] = {
          name: hostname,
        }
      })
    })

    return hosts
  } catch (error) {
    console.error(error) // eslint-disable-line no-console
    throw error
  }
}

export const getLayouts = () =>
  AJAX({
    method: 'GET',
    resource: 'layouts',
  })

export const getAppsForHost = (proxyLink, host, appLayouts, telegrafDB) => {
  const measurements = appLayouts.map(m => `^${m.measurement}$`).join('|')
  const measurementsToApps = _.zipObject(
    appLayouts.map(m => m.measurement),
    appLayouts.map(({app}) => app)
  )

  return proxy({
    source: proxyLink,
    query: `show series from /${measurements}/ where host = '${host}'`,
    db: telegrafDB,
  }).then(resp => {
    const result = {apps: [], tags: {}}
    const allSeries = _.get(resp, 'data.results.0.series.0.values', [])

    allSeries.forEach(([series]) => {
      const seriesObj = parseSeries(series)
      const measurement = seriesObj.measurement

      result.apps = _.uniq(result.apps.concat(measurementsToApps[measurement]))
      _.assign(result.tags, seriesObj.tags)
    })

    return result
  })
}

export const getAppsForHosts = (proxyLink, hosts, appLayouts, telegrafDB) => {
  const measurements = appLayouts.map(m => `^${m.measurement}$`).join('|')
  const measurementsToApps = _.zipObject(
    appLayouts.map(m => m.measurement),
    appLayouts.map(({app}) => app)
  )

  return proxy({
    source: proxyLink,
    query: `show series from /${measurements}/`,
    db: telegrafDB,
  }).then(resp => {
    const newHosts = Object.assign({}, hosts)
    const allSeries = _.get(
      resp,
      ['data', 'results', '0', 'series', '0', 'values'],
      []
    )

    allSeries.forEach(([series]) => {
      const seriesObj = parseSeries(series)
      const measurement = seriesObj.measurement
      const host = _.get(seriesObj, ['tags', 'host'], '')

      if (!newHosts[host]) {
        return
      }

      if (!newHosts[host].apps) {
        newHosts[host].apps = []
      }

      if (!newHosts[host].tags) {
        newHosts[host].tags = {}
      }

      newHosts[host].apps = _.uniq(
        newHosts[host].apps.concat(measurementsToApps[measurement])
      )
      _.assign(newHosts[host].tags, seriesObj.tags)
    })

    return newHosts
  })
}

export function getMeasurementsForHost(source, host) {
  return proxy({
    source: source.links.proxy,
    query: `SHOW MEASUREMENTS WHERE "host" = '${host}'`,
    db: source.telegraf,
  }).then(({data}) => {
    if (_isEmpty(data) || _hasError(data)) {
      return []
    }

    const series = _.get(data, ['results', '0', 'series', '0'])
    return series.values.map(measurement => {
      return measurement[0]
    })
  })
}

function parseSeries(series) {
  const ident = /\w+/
  const tag = /,?([^=]+)=([^,]+)/

  function parseMeasurement(s, obj) {
    const match = ident.exec(s)
    const measurement = match[0]
    if (measurement) {
      obj.measurement = measurement
    }
    return s.slice(match.index + measurement.length)
  }

  function parseTag(s, obj) {
    const match = tag.exec(s)

    if (match) {
      const kv = match[0]
      const key = match[1]
      const value = match[2]

      if (key) {
        if (!obj.tags) {
          obj.tags = {}
        }
        obj.tags[key] = value
      }
      return s.slice(match.index + kv.length)
    }

    return ''
  }

  let workStr = series.slice()
  const out = {}

  // Consume measurement
  workStr = parseMeasurement(workStr, out)

  // Consume tags
  while (workStr.length > 0) {
    workStr = parseTag(workStr, out)
  }

  return out
}

function _isEmpty(resp) {
  return !resp.results[0].series
}

function _hasError(resp) {
  return !!resp.results[0].error
}
