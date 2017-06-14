export const fixtureStatusPageCells = [
  {
    i: 'alerts-bar-graph',
    isWidget: false,
    x: 0,
    y: 0,
    w: 12,
    h: 4,
    name: 'Alerts – Last 30 Days',
    queries: [
      {
        query:
          'SELECT count("value") AS "count_value" FROM "chronograf"."autogen"."alerts" WHERE time > :dashboardTime: GROUP BY time(1d)',
        label: 'count.value',
        queryConfig: {
          database: 'chronograf',
          measurement: 'alerts',
          retentionPolicy: 'autogen',
          fields: [
            {
              field: 'value',
              funcs: ['count'],
            },
          ],
          tags: {},
          groupBy: {
            time: '1d',
            tags: [],
          },
          areTagsAccepted: false,
          rawText: null,
          range: null,
        },
      },
    ],
    type: 'bar',
    links: {
      self: '/chronograf/v1/status/23/cells/c-bar-graphs-fly',
    },
  },
  {
    i: 'recent-alerts',
    isWidget: true,
    name: 'Recent Alerts',
    type: 'alerts',
    x: 0,
    y: 5,
    w: 6.5,
    h: 7,
  },
  {
    i: 'news-feed',
    isWidget: true,
    name: 'News Feed',
    type: 'news',
    x: 6.5,
    y: 5,
    w: 3,
    h: 7,
  },
  {
    i: 'getting-started',
    isWidget: true,
    name: 'Getting Started',
    type: 'guide',
    x: 9.5,
    y: 5,
    w: 2.5,
    h: 7,
  },
]
