import {parseHandlersFromRule} from 'shared/parsing/parseHandlersFromRule'
import {
  emptyRule,
  emptyConfigResponse,
  rule,
  handlersFromConfig,
  handlersOfKindExpected,
  selectedHandlerExpected,
  handlersOnThisAlertExpected,
} from './constants'

describe('parseHandlersFromRule', () => {
  it('returns empty things if rule is new and config is empty', () => {
    const input1 = emptyRule
    const input2 = emptyConfigResponse
    const {
      handlersOnThisAlert,
      selectedHandler,
      handlersOfKind,
    } = parseHandlersFromRule(input1, input2)

    expect(handlersOnThisAlert).toEqual([])
    expect(selectedHandler).toEqual(null)
    expect(handlersOfKind).toEqual({})
  })

  it('returns values if rule and config are not empty', () => {
    const input1 = rule
    const input2 = handlersFromConfig
    const {
      handlersOnThisAlert,
      selectedHandler,
      handlersOfKind,
    } = parseHandlersFromRule(input1, input2)

    expect(handlersOnThisAlert).toEqual(handlersOnThisAlertExpected)
    expect(selectedHandler).toEqual(selectedHandlerExpected)
    expect(handlersOfKind).toEqual(handlersOfKindExpected)
  })
})
