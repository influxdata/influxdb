import {stripPrefix} from 'src/utils/basepath'

describe('basepath.stripPrefix', () => {
  it('returns unchanged pathname if basepath is empty', () => {
    const basepath = ''
    const pathname = '/sources/1/dashboards/1'

    expect(stripPrefix(pathname, basepath)).toBe(pathname)
  })

  it('returns the stripped pathname when basepath is provided', () => {
    const basepath = '/russ'
    const pathname = '/russ/sources/1/dashboards/1'

    expect(stripPrefix(pathname, basepath)).toBe('/sources/1/dashboards/1')
  })

  it('returns the stripped pathname when basepath is present twice', () => {
    const basepath = '/russ'
    const pathname = '/russ/russ/sources/1/dashboards/1'

    expect(stripPrefix(pathname, basepath)).toBe('/russ/sources/1/dashboards/1')
  })
})
