import {proxy} from 'utils/queryUrlGenerator';

export function getAlerts(proxyLink) {
  return proxy({
    source: proxyLink,
    query: "select host, value, name, time, level from alerts",
    db: "chronograf",
  });
}
