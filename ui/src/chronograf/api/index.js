import AJAX from 'utils/ajax';

export function saveExplorer({panels, queryConfigs, explorerID}) {
  return AJAX({
    url: `/api/int/v1/explorers/${explorerID}`,
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    data: JSON.stringify({
      data: JSON.stringify({panels, queryConfigs}),
    }),
  });
}

