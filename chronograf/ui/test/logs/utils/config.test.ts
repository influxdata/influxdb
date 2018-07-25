import {
  logConfigServerToUI,
  logConfigUIToServer,
  columnServerToUI,
  getFormatFromColumn,
  sortColumns,
  getDisplayAndVisibleEncodings,
  getLabelEncodings,
  getColorEncodings,
  getFullEncodings,
  getLevelColorsFromColumn,
} from 'src/logs/utils/config'
import {serverLogConfig, serverLogColumns} from 'test/fixtures'
import {
  SeverityFormatOptions,
  SeverityColorOptions,
  SeverityLevelOptions,
} from 'src/logs/constants'
import {LogConfig} from 'src/types/logs'

const sortedServerColumns = () => {
  return [
    {
      name: 'time',
      position: 0,
      encodings: [
        {
          type: 'visibility',
          value: 'hidden',
        },
      ],
    },
    {
      name: 'severity',
      position: 1,
      encodings: [
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
      encodings: [
        {
          type: 'visibility',
          value: 'visible',
        },
      ],
    },
    {
      name: 'message',
      position: 3,
      encodings: [
        {
          type: 'visibility',
          value: 'visible',
        },
      ],
    },
    {
      name: 'facility',
      position: 4,
      encodings: [
        {
          type: 'visibility',
          value: 'visible',
        },
      ],
    },
    {
      name: 'procid',
      position: 5,
      encodings: [
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
      encodings: [
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
      encodings: [
        {
          type: 'visibility',
          value: 'visible',
        },
      ],
    },
  ]
}

describe('Logs.Config', () => {
  describe('logConfigServerToUI', () => {
    it('converts columns to tableColumns', () => {
      const serverColumn = {
        name: 'appname',
        position: 6,
        encodings: [
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
      const serverColumn2 = {
        name: 'procid',
        position: 0,
        encodings: [
          {
            type: 'visibility',
            value: 'hidden',
          },
        ],
      }
      const uiColumn = columnServerToUI(serverColumn)
      const uiColumn2 = columnServerToUI(serverColumn2)
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

    it('gets severity format from columns', () => {
      const serverColumnDotText = {
        name: 'severity',
        position: 2,
        encodings: [
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

      const serverColumnDot = {
        name: 'severity',
        position: 2,
        encodings: [
          {
            type: 'label',
            value: 'icon',
          },
        ],
      }

      const severityFormatDotText = getFormatFromColumn(serverColumnDotText)
      const severityFormatDot = getFormatFromColumn(serverColumnDot)

      expect(severityFormatDotText).toBe(SeverityFormatOptions.dotText)
      expect(severityFormatDot).toBe(SeverityFormatOptions.dot)
    })

    it('sorts the columns by position', () => {
      const sortedColumns = sortColumns(serverLogColumns)
      const expected = sortedServerColumns()

      expect(sortedColumns).toEqual(expected)
    })

    it('gets Severity Color from column', () => {
      const severityColumn = {
        name: 'severity',
        position: 1,
        encodings: [
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
      const convertedColors = getLevelColorsFromColumn(severityColumn)

      const expectedColors = [
        {
          level: SeverityLevelOptions.emerg,
          color: SeverityColorOptions.pineapple,
        },
        {
          level: SeverityLevelOptions.err,
          color: SeverityColorOptions.fire,
        },
      ]

      expect(convertedColors).toEqual(expectedColors)
    })

    it('converts the config from server to the format used by UI', () => {
      const uiLogConfig = logConfigServerToUI(serverLogConfig)

      const expected = {
        tableColumns: [
          {internalName: 'time', displayName: '', visible: false},
          {internalName: 'severity', displayName: '', visible: true},
          {internalName: 'timestamp', displayName: '', visible: true},
          {internalName: 'message', displayName: '', visible: true},
          {internalName: 'facility', displayName: '', visible: true},
          {internalName: 'procid', displayName: 'Proc ID', visible: true},
          {
            internalName: 'appname',
            displayName: 'Application',
            visible: true,
          },
          {internalName: 'host', displayName: '', visible: true},
        ],
        severityFormat: SeverityFormatOptions.dotText,
        severityLevelColors: [
          {
            level: SeverityLevelOptions.alert,
            color: SeverityColorOptions.pearl,
          },
          {
            level: SeverityLevelOptions.warning,
            color: SeverityColorOptions.wolf,
          },
        ],
      }

      expect(uiLogConfig).toEqual(expected)
    })
  })

  describe('logConfigUIToServer', () => {
    it('generates visibility and displayName encodings from column', () => {
      const tableColumn = {
        internalName: 'appname',
        displayName: 'Application',
        visible: true,
      }
      const encodings = getDisplayAndVisibleEncodings(tableColumn)
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

      expect(encodings).toEqual(expected)
    })

    it('generates label encodings from serverFormat', () => {
      const severityFormatDotText = SeverityFormatOptions.dotText
      const severityFromatDot = SeverityFormatOptions.dot

      const encodingsDotText = getLabelEncodings(severityFormatDotText)
      const encodingsDot = getLabelEncodings(severityFromatDot)

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

      expect(encodingsDotText).toEqual(expectedDotText)
      expect(encodingsDot).toEqual(expectedDot)
    })

    it('generates color encodings from severityLevelColors', () => {
      const severityLevelColors = [
        {
          level: SeverityLevelOptions.emerg,
          color: SeverityColorOptions.pearl,
        },
        {
          level: SeverityLevelOptions.alert,
          color: SeverityColorOptions.mist,
        },
        {level: SeverityLevelOptions.crit, color: SeverityColorOptions.wolf},
        {
          level: SeverityLevelOptions.err,
          color: SeverityColorOptions.graphite,
        },
      ]

      const colorEncodings = getColorEncodings(severityLevelColors)
      const expectedEncodings = [
        {
          type: 'color',
          name: 'emerg',
          value: SeverityColorOptions.pearl,
        },
        {
          type: 'color',
          name: 'alert',
          value: SeverityColorOptions.mist,
        },
        {
          type: 'color',
          name: 'crit',
          value: SeverityColorOptions.wolf,
        },
        {
          type: 'color',
          name: 'err',
          value: SeverityColorOptions.graphite,
        },
      ]

      expect(colorEncodings).toEqual(expectedEncodings)
    })

    it('gets all encodings when appropriate', () => {
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
      const severityFormat = SeverityFormatOptions.dotText
      const severityLevelColors = [
        {level: SeverityLevelOptions.emerg, color: SeverityColorOptions.pearl},
        {level: SeverityLevelOptions.alert, color: SeverityColorOptions.mist},
        {level: SeverityLevelOptions.crit, color: SeverityColorOptions.wolf},
        {level: SeverityLevelOptions.err, color: SeverityColorOptions.graphite},
      ]
      const encodingsSeverity = getFullEncodings(
        tableColumnSeverity,
        severityFormat,
        severityLevelColors
      )
      const encodingsOther = getFullEncodings(
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
          value: SeverityColorOptions.pearl,
        },
        {
          type: 'color',
          name: 'alert',
          value: SeverityColorOptions.mist,
        },
        {
          type: 'color',
          name: 'crit',
          value: SeverityColorOptions.wolf,
        },
        {
          type: 'color',
          name: 'err',
          value: SeverityColorOptions.graphite,
        },
      ]

      const expectedOther = [
        {
          type: 'visibility',
          value: 'visible',
        },
      ]

      expect(encodingsSeverity).toEqual(expectedSeverity)
      expect(encodingsOther).toEqual(expectedOther)
    })

    it('converts the config from what the UI uses to what the server takes', () => {
      const uiLogConfig: LogConfig = {
        tableColumns: [
          {internalName: 'time', displayName: '', visible: false},
          {internalName: 'severity', displayName: '', visible: true},
          {internalName: 'timestamp', displayName: '', visible: true},
          {internalName: 'message', displayName: '', visible: true},
          {internalName: 'facility', displayName: '', visible: true},
          {internalName: 'procid', displayName: 'Proc ID', visible: true},
          {
            internalName: 'appname',
            displayName: 'Application',
            visible: true,
          },
          {internalName: 'host', displayName: '', visible: true},
        ],
        severityFormat: SeverityFormatOptions.dotText,
        severityLevelColors: [
          {
            level: SeverityLevelOptions.alert,
            color: SeverityColorOptions.pearl,
          },
          {
            level: SeverityLevelOptions.warning,
            color: SeverityColorOptions.wolf,
          },
        ],
      }

      const convertedServerLogConfig = logConfigUIToServer(uiLogConfig)
      const expected = {columns: sortedServerColumns()}

      expect(convertedServerLogConfig).toEqual(expected)
    })
  })
})
