import {preventNegativeZero} from 'src/shared/utils/preventNegativeZero'

describe('preventNegativeZero', () => {
  it('should not alter non-zero numbers', () => {
    expect(preventNegativeZero(1)).toEqual(1)
    expect(preventNegativeZero(0.000000000001)).toEqual(0.000000000001)
    expect(preventNegativeZero(-456.78)).toEqual(-456.78)

    let nonZeroString = '0.000000000000000000000000001'
    expect(preventNegativeZero(nonZeroString)).toEqual(nonZeroString)

    nonZeroString = '1234567890'
    expect(preventNegativeZero(nonZeroString)).toEqual(nonZeroString)

    nonZeroString = '-123456789.0001'
    expect(preventNegativeZero(nonZeroString)).toEqual(nonZeroString)
  })
  it('should handle negative zero as a number', () => {
    expect(preventNegativeZero(-0)).toEqual(0)
    expect(preventNegativeZero(-0.0)).toEqual(0)

    // prettier-ignore
    expect(preventNegativeZero(-0.00)).toEqual(0)
    // prettier-ignore
    expect(preventNegativeZero(-0.000)).toEqual(0)
    // prettier-ignore
    expect(preventNegativeZero(-0.0000)).toEqual(0)
    // prettier-ignore
    expect(preventNegativeZero(-0.00)).toEqual(0.00)
    // prettier-ignore
    expect(preventNegativeZero(-0.000)).toEqual(0.000)
    // prettier-ignore
    expect(preventNegativeZero(-0.0000)).toEqual(0.0000)
  })
  it('should handle negative zero as a string', () => {
    expect(preventNegativeZero('-0')).toEqual('0')
    expect(preventNegativeZero('-0.0')).toEqual('0.0')
    expect(preventNegativeZero('-0.00000000')).toEqual('0.00000000')

    expect(preventNegativeZero('-0.0')).not.toEqual('0')
    expect(preventNegativeZero('-0.00000000')).not.toEqual('0')
  })
})
