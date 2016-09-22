import AJAX from 'utils/ajax';

// TODO delete buildInfluxUrl
export function buildInfluxUrl() {
  // // If multiple data nodes are provided, pick one at random to avoid hammering any
  // // single node. This could be smarter and track state in the future, but for now it
  // // gets the job done.
  // let url = `${h}/query?epoch=ms&q=${statement}`;

  // if (database) {
  //   url += `&db=${database}`;
  // }

  // if (retentionPolicy) {
  //   url += `&rp=${retentionPolicy}`;
  // }

  console.log("you dont need the buildinfgluxrul generator delte me");
}

export function proxy({source, query, db, rp}) {
  return AJAX({
    method: 'POST',
    url: source,
    data: {
      query,
      db,
      rp,
    },
  });
}
