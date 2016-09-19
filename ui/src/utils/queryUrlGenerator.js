import AJAX from 'utils/ajax';

export function buildInfluxUrl({host, statement, database, retentionPolicy}) {
  // If multiple data nodes are provided, pick one at random to avoid hammering any
  // single node. This could be smarter and track state in the future, but for now it
  // gets the job done.
  const h = Array.isArray(host) ? selectRandomNode(host) : host;
  let url = `${h}/query?epoch=ms&q=${statement}`;

  if (database) {
    url += `&db=${database}`;
  }

  if (retentionPolicy) {
    url += `&rp=${retentionPolicy}`;
  }

  return encodeURIComponent(url);
}

export function proxy(url, clusterID) {
  return AJAX({
    url: `/clusters/${clusterID}/proxy?proxy_url=${url}`,
  });
}

function selectRandomNode(nodes) {
  const index = Math.floor(Math.random() * nodes.length);
  return nodes[index];
}
