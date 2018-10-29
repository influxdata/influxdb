import {transformFluxLogsResponse} from 'src/logs/utils'
import {fluxResponse} from 'src/logs/utils/fixtures/fluxResponse'

describe('Logs.transformFluxLogsResponse', () => {
  const {tables: fluxResponseTables} = fluxResponse

  it('can transform a Flux server response to a TableData shape', () => {
    const columnNamesToExtract = [
      'appname',
      'facility',
      'host',
      'severity',
      'message',
      'procid',
      'timestamp',
    ]

    const actual = transformFluxLogsResponse(
      fluxResponseTables,
      columnNamesToExtract
    )
    const expected = {
      columns: [
        'appname',
        'facility',
        'host',
        'severity',
        'message',
        'procid',
        'timestamp',
      ],
      values: [
        [
          'testbot-2',
          'NTP subsystem',
          'user.local',
          'alert',
          'this is the first log message\\n',
          '92273',
          '1539285296911000000',
        ],
        [
          'testbot-2',
          'cron',
          'user.local',
          'warning',
          'this is the second\\n',
          '92273',
          '1539285306912000000',
        ],
        [
          'testbot-2',
          'cron',
          'user.local',
          'err',
          'this is the third message\\n',
          '92273',
          '1539285306912000000',
        ],
        [
          'testbot-2',
          'lpr',
          'user.local',
          'crit',
          'This is the fourth message\\n',
          '92273',
          '1539285316917000000',
        ],
        [
          'testbot-2',
          'lpr',
          'user.local',
          'err',
          'This is the fifth message\\n',
          '92273',
          '1539285316917000000',
        ],
      ],
    }

    expect(actual).toEqual(expected)
  })

  it('can transform a Flux server response with different column names to a TableData shape', () => {
    const columnNamesToExtract = [
      'facility',
      'host',
      'severity',
      'message',
      'procid',
      'timestamp',
    ]

    const actual = transformFluxLogsResponse(
      fluxResponseTables,
      columnNamesToExtract
    )
    const expected = {
      columns: [
        'facility',
        'host',
        'severity',
        'message',
        'procid',
        'timestamp',
      ],
      values: [
        [
          'NTP subsystem',
          'user.local',
          'alert',
          'this is the first log message\\n',
          '92273',
          '1539285296911000000',
        ],
        [
          'cron',
          'user.local',
          'warning',
          'this is the second\\n',
          '92273',
          '1539285306912000000',
        ],
        [
          'cron',
          'user.local',
          'err',
          'this is the third message\\n',
          '92273',
          '1539285306912000000',
        ],
        [
          'lpr',
          'user.local',
          'crit',
          'This is the fourth message\\n',
          '92273',
          '1539285316917000000',
        ],
        [
          'lpr',
          'user.local',
          'err',
          'This is the fifth message\\n',
          '92273',
          '1539285316917000000',
        ],
      ],
    }

    expect(actual).toEqual(expected)
  })

  it('can extract in the specified column ordering', () => {
    const columnNamesToExtract = ['host', 'facility']

    const actual = transformFluxLogsResponse(
      fluxResponseTables,
      columnNamesToExtract
    )
    const expected = {
      columns: ['host', 'facility'],
      values: [
        ['user.local', 'NTP subsystem'],
        ['user.local', 'cron'],
        ['user.local', 'cron'],
        ['user.local', 'lpr'],
        ['user.local', 'lpr'],
      ],
    }

    expect(actual).toEqual(expected)
  })
})
