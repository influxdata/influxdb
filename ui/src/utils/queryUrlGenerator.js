import AJAX from 'utils/ajax';

// TODO: delete this once all references
// to it have been removed
export function buildInfluxUrl() {
  return "You dont need me anymore";
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
