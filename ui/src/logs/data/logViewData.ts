import {View, LogViewerView, ViewShape, ViewType} from 'src/types/v2/dashboards'
import {LOG_VIEW_NAME} from 'src/logs/constants'

export const logViewProperties: LogViewerView = {
  type: ViewType.LogViewer,
  shape: ViewShape.ChronografV2,
  columns: [
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
          name: 'emerg',
          value: 'ruby',
        },
        {
          type: 'color',
          name: 'alert',
          value: 'fire',
        },
        {
          type: 'color',
          name: 'crit',
          value: 'curacao',
        },
        {
          type: 'color',
          name: 'err',
          value: 'tiger',
        },
        {
          type: 'color',
          name: 'warning',
          value: 'pineapple',
        },
        {
          type: 'color',
          name: 'notice',
          value: 'rainforest',
        },
        {
          type: 'color',
          name: 'info',
          value: 'star',
        },
        {
          type: 'color',
          name: 'debug',
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
    {
      name: 'hostname',
      position: 7,
      settings: [
        {
          type: 'visibility',
          value: 'visible',
        },
      ],
    },
    {
      name: 'host',
      position: 8,
      settings: [
        {
          type: 'visibility',
          value: 'hidden',
        },
      ],
    },
  ],
}

export const logViewData: View = {
  id: null,
  name: LOG_VIEW_NAME,
  properties: logViewProperties,
}
