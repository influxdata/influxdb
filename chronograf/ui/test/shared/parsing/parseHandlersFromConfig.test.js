import parseHandlersFromConfig from 'shared/parsing/parseHandlersFromConfig'
import {
  config,
  configResponse,
  emptyConfig,
  emptyConfigResponse,
} from './constants'

describe('parseHandlersFromConfig', () => {
  it('returns an array', () => {
    const input = config
    const actual = parseHandlersFromConfig(input)
    expect(Array.isArray(actual)).toBe(true)
  })

  it('returns the right response', () => {
    const input = config
    const actual = parseHandlersFromConfig(input)
    expect(actual).toEqual(configResponse)
  })

  it('returns the right response even if config is empty', () => {
    const input = emptyConfig
    const actual = parseHandlersFromConfig(input)
    expect(actual).toEqual(emptyConfigResponse)
  })
})
