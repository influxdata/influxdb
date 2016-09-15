import {proxy, buildInfluxUrl} from 'utils/queryUrlGenerator';

export default function fetchTimeSeries(host, database, query, clusterID) {
  const url = buildInfluxUrl({
    host,
    database,
    statement: query,
  });

  return proxy(url, clusterID);
}
