import {parseHandlersFromRule} from 'shared/parsing/parseHandlersFromRule'
import {
  emptyRule,
  emptyConfigResponse,
  rule,
  handlersFromConfig,
  handlersOfKind_expected,
  selectedHandler_expected,
  handlersOnThisAlert_expected,
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
    const handlersOnThisAlert_expected = []
    const selectedHandler_expected = null
    const handlersOfKind_expected = {}
    expect(handlersOnThisAlert).to.deep.equal(handlersOnThisAlert_expected)
    expect(selectedHandler).to.deep.equal(selectedHandler_expected)
    expect(handlersOfKind).to.deep.equal(handlersOfKind_expected)
  })

  it('returns values if rule and config are not empty', () => {
    const input1 = rule
    const input2 = handlersFromConfig
    const {
      handlersOnThisAlert,
      selectedHandler,
      handlersOfKind,
    } = parseHandlersFromRule(input1, input2)

    expect(handlersOnThisAlert).to.deep.equal(handlersOnThisAlert_expected)
    expect(selectedHandler).to.deep.equal(selectedHandler_expected)
    expect(handlersOfKind).to.deep.equal(handlersOfKind_expected)
  })
})
