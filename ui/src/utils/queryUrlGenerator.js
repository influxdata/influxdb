import AJAX from 'utils/ajax';

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
