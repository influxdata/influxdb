import {proxy} from 'utils/queryUrlGenerator';
import AJAX from 'utils/ajax';
import _ from 'lodash';

export function getAppsForHosts(proxyLink, hosts, appMappings) {
  const measurements = appMappings.map((m) => `^${m.measurement}$`).join('|');
  const measurementsToApps = _.zipObject(appMappings.map(m => m.measurement), appMappings.map(m => m.name));
  return proxy({
    source: proxyLink,
    query: `show series from /${measurements}/`,
    db: 'telegraf',
  }).then((resp) => {
    const newHosts = Object.assign({}, hosts);
    const allSeries = _.get(resp, ['data', 'results', '0', 'series', '0', 'values'], []);
    allSeries.forEach(([series]) => {
      const matches = series.match(/(\w*).*,host=([^,]*)/);
      if (!matches || matches.length !== 3) { // eslint-disable-line no-magic-numbers
        return;
      }
      const measurement = matches[1];
      const host = matches[2];

      if (!newHosts[host]) {
        return;
      }
      if (!newHosts[host].apps) {
        newHosts[host].apps = [];
      }
      newHosts[host].apps = _.uniq(newHosts[host].apps.concat(measurementsToApps[measurement]));
    });

    return newHosts;
  });
}

export function fetchLayouts() {
  return AJAX({
    url: `/chronograf/v1/layouts`,
    method: 'GET',
  });
}
