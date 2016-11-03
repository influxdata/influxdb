import {proxy} from 'utils/queryUrlGenerator';

export function getAlerts(proxyLink) {
  return proxy({
    source: proxyLink,
    query: "select host, value, level, \"name\" from alert",
    db: "chronograf",
  });
}
