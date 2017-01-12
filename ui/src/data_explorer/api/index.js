import AJAX from 'utils/ajax';

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
