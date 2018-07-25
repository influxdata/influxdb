import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'
import {TEMP_VAR_DASHBOARD_TIME} from 'src/shared/constants'
import {NEW_DEFAULT_DASHBOARD_CELL} from 'src/dashboards/constants/index'
import {DEFAULT_AXIS} from 'src/dashboards/constants/cellEditor'
import {Cell, CellQuery, Axes, CellType} from 'src/types'

const emptyQuery: CellQuery = {
  query: '',
  source: '',
  queryConfig: {
    database: '',
    measurement: '',
    retentionPolicy: '',
    fields: [],
    tags: {},
    groupBy: {},
    areTagsAccepted: false,
    rawText: null,
    range: null,
  },
}

const emptyAxes: Axes = {
  x: DEFAULT_AXIS,
  y: DEFAULT_AXIS,
  y2: DEFAULT_AXIS,
}

export const fixtureStatusPageCells: Cell[] = [
  {
    ...NEW_DEFAULT_DASHBOARD_CELL,
    axes: emptyAxes,
    i: 'alerts-bar-graph',
    type: CellType.Bar,
    isWidget: false,
    x: 0,
    y: 0,
    w: 12,
    h: 4,
    legend: {},
    name: 'Alert Events per Day – Last 30 Days',
    colors: DEFAULT_LINE_COLORS,
    queries: [
      {
        query: `SELECT count("value") AS "count_value" FROM "chronograf"."autogen"."alerts" WHERE time > ${TEMP_VAR_DASHBOARD_TIME} GROUP BY time(1d)`,
        source: '',
        queryConfig: {
          database: 'chronograf',
          measurement: 'alerts',
          retentionPolicy: 'autogen',
          fields: [
            {
              value: 'count',
              type: 'func',
              alias: 'count_value',
              args: [
                {
                  value: 'value',
                  type: 'field',
                },
              ],
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
    links: {
      self: '/chronograf/v1/status/23/cells/c-bar-graphs-fly',
    },
  },
  {
    ...NEW_DEFAULT_DASHBOARD_CELL,
    axes: emptyAxes,
    i: 'recent-alerts',
    type: CellType.Alerts,
    isWidget: true,
    name: 'Alerts – Last 30 Days',
    x: 0,
    y: 5,
    w: 6.5,
    h: 6,
    legend: {},
    queries: [emptyQuery],
    colors: DEFAULT_LINE_COLORS,
    links: {self: ''},
  },
  {
    ...NEW_DEFAULT_DASHBOARD_CELL,
    axes: emptyAxes,
    i: 'news-feed',
    type: CellType.News,
    isWidget: true,
    name: 'News Feed',
    x: 6.5,
    y: 5,
    w: 3,
    h: 6,
    legend: {},
    queries: [emptyQuery],
    colors: DEFAULT_LINE_COLORS,
    links: {self: ''},
  },
  {
    ...NEW_DEFAULT_DASHBOARD_CELL,
    axes: emptyAxes,
    i: 'getting-started',
    type: CellType.Guide,
    isWidget: true,
    name: 'Getting Started',
    x: 9.5,
    y: 5,
    w: 2.5,
    h: 6,
    legend: {},
    queries: [emptyQuery],
    colors: DEFAULT_LINE_COLORS,
    links: {self: ''},
  },
]
