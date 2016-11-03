import {proxy} from 'utils/queryUrlGenerator';

export function getAlerts(proxyLink) {
  return proxy({
    source: proxyLink,
    query: "select host, value, level, alert_name from alert",
    db: "chronograf",
  });
}
