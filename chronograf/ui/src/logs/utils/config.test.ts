import {
  serverToUIConfig,
  uiToServerConfig,
  generateColumnConfig,
  generateColumnFormatConfig,
  sortViewColumns,
  generateViewColumns,
  generateViewColumnSeverityLabels,
  generateViewColumnSeverityColors,
  generateViewColumnSettings,
  generateColumnColorsConfig,
} from 'src/logs/utils/config'

import {
  logView,
  logViewerColumns,
  logViewID,
  logViewSelfLink,
} from 'src/logs/utils/fixtures/logView'

import {
  SeverityFormatOptions,
  SeverityColorOptions,
  SeverityLevelOptions,
  LogConfig,
} from 'src/types/logs'

import {ViewType, ViewShape} from 'src/types/v2/dashboards'

const sortLogViewColumns = () => {
  return [
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
      name: 'host',
      position: 7,
      settings: [
        {
          type: 'visibility',
          value: 'visible',
        },
      ],
    },
  ]
}

describe('Logs.Config', () => {
  describe('serverToUIConfig', () => {
    it('converts columns to tableColumns', () => {
      const viewerColumn = {
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
      }
      const viewerColumn2 = {
        name: 'procid',
        position: 0,
        settings: [
          {
            type: 'visibility',
            value: 'hidden',
          },
        ],
      }
      const uiColumn = generateColumnConfig(viewerColumn)
      const uiColumn2 = generateColumnConfig(viewerColumn2)
      const expectedColumn = {
        internalName: 'appname',
        displayName: 'Application',
        visible: true,
      }
      const expectedColumn2 = {
        internalName: 'procid',
        displayName: '',
        visible: false,
      }

      expect(uiColumn).toEqual(expectedColumn)
      expect(uiColumn2).toEqual(expectedColumn2)
    })

    it('generates severity format from columns', () => {
      const viewDotText = {
        name: 'severity',
        position: 2,
        settings: [
          {
            type: 'label',
            value: 'icon',
          },
          {
            type: 'label',
            value: 'text',
          },
        ],
      }

      const viewColumnDot = {
        name: 'severity',
        position: 2,
        settings: [
          {
            type: 'label',
            value: 'icon',
          },
        ],
      }

      const severityFormatDotText = generateColumnFormatConfig(viewDotText)
      const severityFormatDot = generateColumnFormatConfig(viewColumnDot)

      expect(severityFormatDotText).toBe(SeverityFormatOptions.DotText)
      expect(severityFormatDot).toBe(SeverityFormatOptions.Dot)
    })

    it('sorts columns by column position', () => {
      const sortedColumns = sortViewColumns(logViewerColumns)
      const expected = sortLogViewColumns()

      expect(sortedColumns).toEqual(expected)
    })

    it('generates severity color from column settings', () => {
      const severityColumn = {
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
            value: 'pineapple',
          },
          {
            type: 'color',
            name: 'err',
            value: 'fire',
          },
        ],
      }
      const convertedColors = generateColumnColorsConfig(severityColumn)

      const expectedColors = [
        {
          level: SeverityLevelOptions.Emerg,
          color: SeverityColorOptions.Pineapple,
        },
        {
          level: SeverityLevelOptions.Err,
          color: SeverityColorOptions.Fire,
        },
      ]

      expect(convertedColors).toEqual(expectedColors)
    })

    it('converts the config from server to the format used by UI', () => {
      const uiLogConfig = serverToUIConfig(logView)

      const expected = {
        id: logViewID,
        link: logViewSelfLink,
        isTruncated: true,
        tableColumns: [
          {internalName: 'time', displayName: '', visible: false},
          {internalName: 'severity', displayName: '', visible: true},
          {internalName: 'timestamp', displayName: '', visible: true},
          {internalName: 'message', displayName: '', visible: true},
          {internalName: 'facility', displayName: '', visible: true},
          {internalName: 'procid', displayName: 'Proc ID', visible: true},
          {internalName: 'appname', displayName: 'Application', visible: true},
          {internalName: 'host', displayName: '', visible: true},
        ],
        severityFormat: SeverityFormatOptions.DotText,
        severityLevelColors: [
          {
            level: SeverityLevelOptions.Alert,
            color: SeverityColorOptions.Pearl,
          },
          {
            level: SeverityLevelOptions.Warning,
            color: SeverityColorOptions.Wolf,
          },
        ],
      }

      expect(uiLogConfig).toEqual(expected)
    })
  })

  describe('uiToServerConfig', () => {
    it('generates visibility and displayName settings from column', () => {
      const tableColumn = {
        internalName: 'appname',
        displayName: 'Application',
        visible: true,
      }
      const settings = generateViewColumns(tableColumn)
      const expected = [
        {
          type: 'visibility',
          value: 'visible',
        },
        {
          type: 'displayName',
          value: 'Application',
        },
      ]

      expect(settings).toEqual(expected)
    })

    it('generates label settings from view column settings', () => {
      const severityFormatDotText = SeverityFormatOptions.DotText
      const severityFormatDot = SeverityFormatOptions.Dot

      const settingsDotText = generateViewColumnSeverityLabels(
        severityFormatDotText
      )
      const settingsDot = generateViewColumnSeverityLabels(severityFormatDot)

      const expectedDotText = [
        {
          type: 'label',
          value: 'icon',
        },
        {
          type: 'label',
          value: 'text',
        },
      ]
      const expectedDot = [
        {
          type: 'label',
          value: 'icon',
        },
      ]

      expect(settingsDotText).toEqual(expectedDotText)
      expect(settingsDot).toEqual(expectedDot)
    })

    it('generates color settings from severityLevelColors', () => {
      const severityLevelColors = [
        {
          level: SeverityLevelOptions.Emerg,
          color: SeverityColorOptions.Pearl,
        },
        {
          level: SeverityLevelOptions.Alert,
          color: SeverityColorOptions.Mist,
        },
        {
          level: SeverityLevelOptions.Crit,
          color: SeverityColorOptions.Wolf,
        },
        {
          level: SeverityLevelOptions.Err,
          color: SeverityColorOptions.Graphite,
        },
      ]

      const colorSettings = generateViewColumnSeverityColors(
        severityLevelColors
      )
      const expectedSettings = [
        {
          type: 'color',
          name: 'emerg',
          value: SeverityColorOptions.Pearl,
        },
        {
          type: 'color',
          name: 'alert',
          value: SeverityColorOptions.Mist,
        },
        {
          type: 'color',
          name: 'crit',
          value: SeverityColorOptions.Wolf,
        },
        {
          type: 'color',
          name: 'err',
          value: SeverityColorOptions.Graphite,
        },
      ]

      expect(colorSettings).toEqual(expectedSettings)
    })

    it('gets all settings when appropriate', () => {
      const displayName = 'SEVERITY'
      const tableColumnSeverity = {
        internalName: 'severity',
        displayName,
        visible: true,
      }
      const tableColumnOther = {
        internalName: 'host',
        displayName: '',
        visible: true,
      }
      const severityFormat = SeverityFormatOptions.DotText
      const severityLevelColors = [
        {level: SeverityLevelOptions.Emerg, color: SeverityColorOptions.Pearl},
        {level: SeverityLevelOptions.Alert, color: SeverityColorOptions.Mist},
        {level: SeverityLevelOptions.Crit, color: SeverityColorOptions.Wolf},
        {level: SeverityLevelOptions.Err, color: SeverityColorOptions.Graphite},
      ]
      const settingsSeverity = generateViewColumnSettings(
        tableColumnSeverity,
        severityFormat,
        severityLevelColors
      )
      const settingsOther = generateViewColumnSettings(
        tableColumnOther,
        severityFormat,
        severityLevelColors
      )
      const expectedSeverity = [
        {
          type: 'visibility',
          value: 'visible',
        },
        {
          type: 'displayName',
          value: displayName,
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
          value: SeverityColorOptions.Pearl,
        },
        {
          type: 'color',
          name: 'alert',
          value: SeverityColorOptions.Mist,
        },
        {
          type: 'color',
          name: 'crit',
          value: SeverityColorOptions.Wolf,
        },
        {
          type: 'color',
          name: 'err',
          value: SeverityColorOptions.Graphite,
        },
      ]

      const expectedOther = [
        {
          type: 'visibility',
          value: 'visible',
        },
      ]

      expect(settingsSeverity).toEqual(expectedSeverity)
      expect(settingsOther).toEqual(expectedOther)
    })

    it('converts from UI to server config', () => {
      const uiLogConfig: LogConfig = {
        id: logViewID,
        link: logViewSelfLink,
        isTruncated: true,
        tableColumns: [
          {internalName: 'time', displayName: '', visible: false},
          {internalName: 'severity', displayName: '', visible: true},
          {internalName: 'timestamp', displayName: '', visible: true},
          {internalName: 'message', displayName: '', visible: true},
          {internalName: 'facility', displayName: '', visible: true},
          {internalName: 'procid', displayName: 'Proc ID', visible: true},
          {internalName: 'appname', displayName: 'Application', visible: true},
          {internalName: 'host', displayName: '', visible: true},
        ],
        severityFormat: SeverityFormatOptions.DotText,
        severityLevelColors: [
          {
            level: SeverityLevelOptions.Alert,
            color: SeverityColorOptions.Pearl,
          },
          {
            level: SeverityLevelOptions.Warning,
            color: SeverityColorOptions.Wolf,
          },
        ],
      }

      const convertedLogView = uiToServerConfig(uiLogConfig)
      const expected = {
        id: logViewID,
        name: 'LOGS_PAGE',
        properties: {
          type: ViewType.LogViewer,
          shape: ViewShape.ChronografV2,
          columns: sortLogViewColumns(),
        },
      }

      expect(convertedLogView).toEqual(expected)
    })
  })
})
