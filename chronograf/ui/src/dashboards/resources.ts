export const dashboard = {
  id: '1',
  name: 'd1',
  cells: [
    {
      x: 1,
      y: 2,
      w: 3,
      h: 4,
      id: '1',
      viewID: '2',
      links: {
        self: '/v2/dashboards/1/cells/1',
        view: '/v2/dashboards/1/cells/1/views',
        copy: '/v2/dashboards/1/cells/1/copy',
      },
    },
  ],
  links: {
    self: '/v2/dashboards/1',
    cells: '/v2/dashboards/cells',
  },
}
