export const EMPTY_DASHBOARD = {
  id: 0,
  name: '',
  cells: [
    {
      x: 0,
      y: 0,
      queries: [],
      name: 'Loading...',
      type: 'single-stat',
    },
  ],
}

export const NEW_DASHBOARD = {
  name: 'Name This Dashboard',
  cells: [
    {
      x: 0,
      y: 0,
      w: 4,
      h: 4,
      name: 'Name This Graph',
      queries: [
        {
          query: "SELECT mean(\"usage_user\") AS \"usage_user\" FROM \"cpu\"",
          label: "",
          groupbys: [],
          wheres: [],
        },
      ],
    },
  ],
}
