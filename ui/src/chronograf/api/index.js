import AJAX from 'utils/ajax';
import {proxy} from 'utils/queryUrlGenerator';

export function saveExplorer({name, panels, queryConfigs, explorerID}) {
  return AJAX({
    url: explorerID,
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
    },
    data: JSON.stringify({
      data: JSON.stringify({panels, queryConfigs}),
      name,
    }),
  });
}

export function verifySource(proxyLink) {
  return proxy({
    source: proxyLink,
    query: "select * from cpu limit 1",
    db: 'telegraf',
  });
}
