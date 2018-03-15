import uuid from 'uuid'

import {isUUIDv4} from 'src/utils/stringValidators'

describe('isUUIDv4', () => {
  it('returns false for non-matches', () => {
    const inputWrongLength = 'abcd'
    expect(isUUIDv4(inputWrongLength)).toEqual(false)

    const inputWrongFormat = 'abcdefghijklmnopqrstuvwxyz1234567890'
    expect(isUUIDv4(inputWrongFormat)).toEqual(false)

    const inputWrongCharRange = 'z47ac10b-58cc-4372-a567-0e02b2c3d479'
    expect(isUUIDv4(inputWrongCharRange)).toEqual(false)
  })

  it('returns true for matches', () => {
    const inputRight = 'a47ac10b-58cc-4372-a567-0e02b2c3d479'
    expect(isUUIDv4(inputRight)).toEqual(true)

    const inputRightFromLibrary = uuid.v4()
    expect(isUUIDv4(inputRightFromLibrary)).toEqual(true)
  })
})
