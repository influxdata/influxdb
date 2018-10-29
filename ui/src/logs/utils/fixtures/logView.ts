import {LOG_VIEW_NAME} from 'src/logs/constants'

import {
  LogViewerColumn,
  LogViewerView,
  ViewType,
  ViewShape,
  View,
} from 'src/types/v2/dashboards'

export const logViewerColumns: LogViewerColumn[] = [
  {
    name: 'severity',
    position: 1,
    settings: [
      {
        type: 'visibility',
        value: 'visible',
      },
      {
        type: 'label',
        value: 'icon',
      },
      {
        type: 'label',
        value: 'text',
      },
      {
        type: 'color',
        name: 'alert',
        value: 'pearl',
      },
      {
        type: 'color',
        name: 'warning',
        value: 'wolf',
      },
    ],
  },
  {
    name: 'timestamp',
    position: 2,
    settings: [
      {
        type: 'visibility',
        value: 'visible',
      },
    ],
  },
  {
    name: 'message',
    position: 3,
    settings: [
      {
        type: 'visibility',
        value: 'visible',
      },
    ],
  },
  {
    name: 'facility',
    position: 4,
    settings: [
      {
        type: 'visibility',
        value: 'visible',
      },
    ],
  },
  {
    name: 'time',
    position: 0,
    settings: [
      {
        type: 'visibility',
        value: 'hidden',
      },
    ],
  },
  {
    name: 'procid',
    position: 5,
    settings: [
      {
        type: 'visibility',
        value: 'visible',
      },
      {
        type: 'displayName',
        value: 'Proc ID',
      },
    ],
  },
  {
    name: 'host',
    position: 7,
    settings: [
      {
        type: 'visibility',
        value: 'visible',
      },
    ],
  },
  {
    name: 'appname',
    position: 6,
    settings: [
      {
        type: 'visibility',
        value: 'visible',
      },
      {
        type: 'displayName',
        value: 'Application',
      },
    ],
  },
]

export const logViewProperties: LogViewerView = {
  type: ViewType.LogViewer,
  shape: ViewShape.ChronografV2,
  columns: logViewerColumns,
}

export const logViewID = '02bca872cc18d000'
export const logViewSelfLink = '/api/v2/views/02bca872cc18d000'
export const logViewLinks = {
  self: logViewSelfLink,
}

export const logView: View = {
  id: logViewID,
  links: logViewLinks,
  name: LOG_VIEW_NAME,
  properties: logViewProperties,
}
