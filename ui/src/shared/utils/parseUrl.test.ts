import {parseUrl} from 'src/shared/utils/parseUrl'

describe('Shared.utils.parseUrl', () => {
  it('should return the current domain if none is provided', () => {
    const actual = parseUrl(undefined)
    expect(actual).toEqual('localhost/')
  })

  it('should return the current domain if an empty string is provided', () => {
    const actual = parseUrl('')
    expect(actual).toEqual('localhost/')
  })

  it('Should return the url stripped of the query and protocol', () => {
    const actual = parseUrl('https://www.influxdata.com/developers?random')
    expect(actual).toEqual('www.influxdata.com/developers')
  })

  it('Should return the url when no protocol has been provided', () => {
    const actual = parseUrl('www.influxdata.com/developers')
    expect(actual).toEqual('www.influxdata.com/developers')
  })
})
