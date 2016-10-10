import {proxy} from 'utils/queryUrlGenerator';
import _ from 'lodash';

export function getCpuAndLoadForHosts(proxyLink) {
  return proxy({
    source: proxyLink,
    query: `select mean(usage_user) from cpu where cpu = 'cpu-total' and time > now() - 10m group by host; select mean("load1") from "telegraf"."default"."system" where time > now() - 10m group by host`,
    db: 'telegraf',
  }).then((resp) => {
    const hosts = {};
    const precision = 100;
    resp.data.results[0].series.forEach((s) => {
      const meanIndex = s.columns.findIndex((col) => col === 'mean');
      hosts[s.tags.host] = {
        name: s.tags.host,
        cpu: (Math.round(s.values[0][meanIndex] * precision) / precision).toFixed(2),
      };
    });

    resp.data.results[1].series.forEach((s) => {
      const meanIndex = s.columns.findIndex((col) => col === 'mean');
      hosts[s.tags.host].load = (Math.round(s.values[0][meanIndex] * precision) / precision).toFixed(2);
    });

    return _.values(hosts);
  });
}
