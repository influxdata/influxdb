import {formatNumber} from 'src/shared/utils/formatNumber'

describe('formatNumber', () => {
  test('can format numbers with no unit prefixes', () => {
    expect(formatNumber(123456789.123456789)).toEqual('123456789.1235')
  })

  test('can format numbers with SI unit prefixes', () => {
    expect(formatNumber(123456, '10')).toEqual('123.5k')
    expect(formatNumber(123456789.123456789, '10')).toEqual('123.5M')
    expect(formatNumber(12345678912345.123456789, '10')).toEqual('12.35T')
  })

  test('can format numbers with binary unit prefixes', () => {
    expect(formatNumber(2 ** 10, '2')).toEqual('1K')
    expect(formatNumber(2 ** 20, '2')).toEqual('1M')
    expect(formatNumber(2 ** 30, '2')).toEqual('1G')
  })

  test('can format negative numbers with a binary unit prefix', () => {
    expect(formatNumber(0 - 2 ** 30, '2')).toEqual('-1G')
  })

  test('formats small numbers without unit prefixes', () => {
    expect(formatNumber(0.551249, '2')).toEqual('0.5512')
    expect(formatNumber(0.551249, '10')).toEqual('0.5512')
  })
})
