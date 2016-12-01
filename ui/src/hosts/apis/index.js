import {proxy} from 'utils/queryUrlGenerator';
import AJAX from 'utils/ajax';
import _ from 'lodash';

export function getCpuAndLoadForHosts(proxyLink, telegrafDB) {
  return proxy({
    source: proxyLink,
    query: `select mean(usage_user) from cpu where cpu = 'cpu-total' and time > now() - 10m group by host; select mean("load1") from "system" where time > now() - 10m group by host; select mean("Percent_Processor_Time") from win_cpu where time > now() - 10m group by host; select mean("Processor_Queue_Length") from win_system where time > now() - 10s group by host`,
    db: telegrafDB,
  }).then((resp) => {
    const hosts = {};
    const precision = 100;
    const cpuSeries = _.get(resp, ['data', 'results', '0', 'series'], []);
    const loadSeries = _.get(resp, ['data', 'results', '1', 'series'], []);
    const winCPUSeries = _.get(resp, ['data', 'results', '2', 'series'], []);
    const winLoadSeries = _.get(resp, ['data', 'results', '3', 'series'], []);
    cpuSeries.forEach((s) => {
      const meanIndex = s.columns.findIndex((col) => col === 'mean');
      hosts[s.tags.host] = {
        name: s.tags.host,
        cpu: (Math.round(s.values[0][meanIndex] * precision) / precision),
      };
    });

    loadSeries.forEach((s) => {
      const meanIndex = s.columns.findIndex((col) => col === 'mean');
      hosts[s.tags.host].load = (Math.round(s.values[0][meanIndex] * precision) / precision);
    });

    winCPUSeries.forEach((s) => {
      const meanIndex = s.columns.findIndex((col) => col === 'mean');
      hosts[s.tags.host] = {
        name: s.tags.host,
        cpu: (Math.round(s.values[0][meanIndex] * precision) / precision),
      };
    });

    winLoadSeries.forEach((s) => {
      const meanIndex = s.columns.findIndex((col) => col === 'mean');
      hosts[s.tags.host].load = (Math.round(s.values[0][meanIndex] * precision) / precision);
    });

    return hosts;
  });
}

export function getMappings() {
  return AJAX({
    method: 'GET',
    url: `/chronograf/v1/mappings`,
  });
}

export function getAppsForHosts(proxyLink, hosts, appMappings, telegrafDB) {
  const measurements = appMappings.map((m) => `^${m.measurement}$`).join('|');
  const measurementsToApps = _.zipObject(appMappings.map(m => m.measurement), appMappings.map(m => m.name));
  return proxy({
    source: proxyLink,
    query: `show series from /${measurements}/`,
    db: telegrafDB,
  }).then((resp) => {
    const newHosts = Object.assign({}, hosts);
    const allSeries = _.get(resp, ['data', 'results', '0', 'series', '0', 'values'], []);
    allSeries.forEach(([series]) => {
      const seriesObj = parseSeries(series);
      const measurement = seriesObj.measurement;
      const host = seriesObj.tags.host;

      if (!newHosts[host]) {
        return;
      }
      if (!newHosts[host].apps) {
        newHosts[host].apps = [];
      }
      if (!newHosts[host].tags) {
        newHosts[host].tags = {};
      }
      newHosts[host].apps = _.uniq(newHosts[host].apps.concat(measurementsToApps[measurement]));
      _.assign(newHosts[host].tags, seriesObj.tags);
    });

    return newHosts;
  });
}

export function getMeasurementsForHost(source, host) {
  return proxy({
    source: source.links.proxy,
    query: `SHOW MEASUREMENTS WHERE "host" = '${host}'`,
    db: source.telegraf,
  }).then(({data}) => {
    if (_isEmpty(data) || _hasError(data)) {
      return [];
    }

    const series = data.results[0].series[0];
    return series.values.map((measurement) => {
      return measurement[0];
    });
  });
}

function parseSeries(series) {
  const ident = /\w+/;
  const tag = /,?([^=]+)=([^,]+)/;

  function parseMeasurement(s, obj) {
    const match = ident.exec(s);
    const measurement = match[0];
    if (measurement) {
      obj.measurement = measurement;
    }
    return s.slice(match.index + measurement.length);
  }

  function parseTag(s, obj) {
    const match = tag.exec(s);

    const kv = match[0];
    const key = match[1];
    const value = match[2];

    if (key) {
      if (!obj.tags) {
        obj.tags = {};
      }
      obj.tags[key] = value;
    }
    return s.slice(match.index + kv.length);
  }

  let workStr = series.slice();
  const out = {};

  // Consume measurement
  workStr = parseMeasurement(workStr, out);

  // Consume tags
  while (workStr.length > 0) {
    workStr = parseTag(workStr, out);
  }

  return out;
}

function _isEmpty(resp) {
  return !resp.results[0].series;
}

function _hasError(resp) {
  return !!resp.results[0].error;
}
