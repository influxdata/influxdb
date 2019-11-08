import React from 'react'
import {convertUserInputToNumOrNaN} from 'src/shared/utils/convertUserInput'

describe('convertUserInputToNumOrNaN', () => {
  let value
  let result

  it('should convert a number string to a number', () => {
    value = 123
    const event = {
      target: {
        value: `${value}`,
      },
    } as React.ChangeEvent<HTMLInputElement>
    expect(convertUserInputToNumOrNaN(event)).toEqual(value)
  })

  it('should convert a non-numeric, non-empty string to NaN', () => {
    const event = {
      target: {
        value: 'a number',
      },
    } as React.ChangeEvent<HTMLInputElement>

    result = convertUserInputToNumOrNaN(event)
    expect(Number.isNaN(result)).toEqual(true)
  })

  it('should convert an empty string to NaN', () => {
    const event = {
      target: {
        value: '',
      },
    } as React.ChangeEvent<HTMLInputElement>

    result = convertUserInputToNumOrNaN(event)
    expect(Number.isNaN(result)).toEqual(true)
  })

  it('should convert undefined to NaN', () => {
    const event = {
      target: {
        value: undefined,
      },
    } as React.ChangeEvent<HTMLInputElement>
    result = convertUserInputToNumOrNaN(event)
    expect(Number.isNaN(result)).toEqual(true)
  })

  it('should convert false to 0', () => {
    value = false
    const event = {
      target: {
        value,
      },
    } as React.ChangeEvent<HTMLInputElement>
    expect(convertUserInputToNumOrNaN(event)).toEqual(0)
  })

  it('should convert null to 0', () => {
    value = null
    const event = {
      target: {
        value,
      },
    } as React.ChangeEvent<HTMLInputElement>
    expect(convertUserInputToNumOrNaN(event)).toEqual(0)
  })
})
