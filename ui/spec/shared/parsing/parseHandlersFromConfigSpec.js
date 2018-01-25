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
    expect(actual).to.be.a('array')
  })
  it('returns the right response', () => {
    const input = config
    const actual = parseHandlersFromConfig(input)
    expect(actual).to.deep.equal(configResponse)
  })
  it('returns the right response even if config is empty', () => {
    const input = emptyConfig
    const actual = parseHandlersFromConfig(input)
    expect(actual).to.deep.equal(emptyConfigResponse)
  })
})
