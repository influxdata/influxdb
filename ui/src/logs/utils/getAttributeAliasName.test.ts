import {getAttributeAliasName} from 'src/logs/utils/search'

describe('Logs.search.getAttributeAliasName', () => {
  it('can convert appname aliases', () => {
    const actual = [
      getAttributeAliasName('ap'),
      getAttributeAliasName('app'),
      getAttributeAliasName('apps'),
      getAttributeAliasName('application'),
      getAttributeAliasName('APP'),
      getAttributeAliasName('Application'),
      getAttributeAliasName('program'),
    ]

    const expected = Array(actual.length).fill('appname')

    expect(actual).toEqual(expected)
  })

  it('can handle unaliased attributes', () => {
    const actual = [
      getAttributeAliasName('poof'),
      getAttributeAliasName('zoom'),
    ]

    const expected = ['poof', 'zoom']

    expect(actual).toEqual(expected)
  })
})
