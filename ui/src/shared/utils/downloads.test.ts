import {formatDownloadName} from './download'

describe('formatDownloadName', () => {
  it('formats name correctly', () => {
    const name1 = 'My Dashboard '
    const name2 = 'my_dash'
    const name3 = 'SystemConfig'

    const expected1 = 'my_dashboard.json'
    const expected2 = 'my_dash.json'
    const expected3 = 'systemconfig.toml'

    const extension = '.json'
    const extension2 = '.toml'
    const actual1 = formatDownloadName(name1, extension)
    const actual2 = formatDownloadName(name2, extension)
    const actual3 = formatDownloadName(name3, extension2)

    expect(actual1).toEqual(expected1)
    expect(actual2).toEqual(expected2)
    expect(actual3).toEqual(expected3)
  })
})
