import {formatStatValue} from './formatStatValue'

describe('formatStatValue', () => {
  let prefix: string
  let value: number | string
  let suffix: string

  describe('handles bad input gracefully', () => {
    test('does not throw an error when decimal places is greater than 100', () => {
      expect(() =>
        formatStatValue('2.00', {
          decimalPlaces: {isEnforced: true, digits: 101},
        })
      ).not.toThrow()
    })

    test('handles undefined', () => {
      prefix = 'LOL'
      value = undefined
      suffix = ' is funny'

      const errorResult = formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: 0},
        prefix,
        suffix,
      })
      expect(typeof errorResult === 'string').toEqual(true)
      expect(errorResult).not.toEqual(`${prefix}${value}${suffix}`)
    })

    test('handles null', () => {
      prefix = ':-('
      value = null
      suffix = ' is not funny'

      const errorResult = formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: 0},
        prefix,
        suffix,
      })
      expect(typeof errorResult === 'string').toEqual(true)
      expect(errorResult).not.toEqual(`${prefix}${value}${suffix}`)
    })
  })

  test('formats NaN', () => {
    prefix = 'My nanny is '
    value = NaN
    suffix = ''
    expect(
      formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: 0},
        prefix,
        suffix,
      })
    ).toEqual(`${prefix}${value}${suffix}`)
  })

  test('formats Infinity', () => {
    prefix = 'To '
    value = Infinity
    suffix = ' and beyond'

    const INFINITY_SYMBOL = '∞'
    expect(
      formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: 0},
        prefix,
        suffix,
      })
    ).toEqual(`${prefix}${INFINITY_SYMBOL}${suffix}`)
  })

  test('formats negative Infinity', () => {
    prefix = 'To '
    value = -Infinity
    suffix = ' and beyond'

    const INFINITY_SYMBOL = '∞'
    expect(
      formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: 0},
        prefix,
        suffix,
      })
    ).toEqual(`${prefix}-${INFINITY_SYMBOL}${suffix}`)
  })

  test('formats 0 with trailing decimal 0s', () => {
    prefix = ''
    value = 0
    let trailingDecimals = 2
    suffix = ''

    expect(
      formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: trailingDecimals},
        prefix,
        suffix,
      })
    ).toEqual(`${prefix}0.00${suffix}`)

    trailingDecimals = 10
    expect(
      formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: trailingDecimals},
        prefix,
        suffix,
      })
    ).toEqual(`${prefix}0.0000000000${suffix}`)
  })

  test('formats an integer value', () => {
    prefix = 'we have '
    value = 123
    suffix = ' abc'
    expect(
      formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: 0},
        prefix,
        suffix,
      })
    ).toEqual(`${prefix}${value}${suffix}`)

    value = -1
    expect(
      formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: 0},
        prefix,
        suffix,
      })
    ).toEqual(`${prefix}${value}${suffix}`)

    value = 123456789
    expect(
      formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: 0},
        prefix,
        suffix,
      })
    ).toEqual(`${prefix}123,456,789${suffix}`)
  })

  test('formats a float value', () => {
    prefix = 'abc '
    value = 123.456789
    suffix = ' yoyoyo'
    expect(
      formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: 6},
        prefix,
        suffix,
      })
    ).toEqual(`${prefix}${value}${suffix}`)

    value = -9.012345678911111111111111111111111111111111111
    expect(
      formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: 20},
        prefix,
        suffix,
      })
    ).toEqual(`${prefix}-9.0123456789${suffix}`)
  })

  test('formats an empty string value', () => {
    prefix = ''
    value = ''
    suffix = ''
    expect(
      formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: 3},
        prefix,
        suffix,
      })
    ).toEqual(`${prefix}${value}${suffix}`)
  })

  test('formats a non-empty string value', () => {
    prefix = 'Negative '
    value = '-666.666'
    suffix = ' is Evil'
    expect(
      formatStatValue(value, {
        decimalPlaces: {isEnforced: true, digits: 3},
        prefix,
        suffix,
      })
    ).toEqual(`${prefix}${value}${suffix}`)
  })

  test('keeps trailing zeroes for the required number of decimal places', () => {
    value = '2.000'
    expect(
      formatStatValue(value, {decimalPlaces: {isEnforced: true, digits: 3}})
    ).toEqual('2.000')

    value = '2.00000000000000000000000000'
    expect(
      formatStatValue(value, {decimalPlaces: {isEnforced: true, digits: 3}})
    ).toEqual('2.000')

    value = '2.0'
    expect(
      formatStatValue(value, {decimalPlaces: {isEnforced: true, digits: 3}})
    ).toEqual('2.000')

    value = '2'
    expect(
      formatStatValue(value, {decimalPlaces: {isEnforced: true, digits: 3}})
    ).toEqual('2.000')

    /* prettier-ignore */
    value = 2
    expect(
      formatStatValue(value, {decimalPlaces: {isEnforced: true, digits: 3}})
    ).toEqual('2.000')

    /* prettier-ignore */
    value = 2.0
    expect(
      formatStatValue(value, {decimalPlaces: {isEnforced: true, digits: 3}})
    ).toEqual('2.000')

    /* prettier-ignore */
    value = 2.00000000
    expect(
      formatStatValue(value, {decimalPlaces: {isEnforced: true, digits: 3}})
    ).toEqual('2.000')
  })
})
