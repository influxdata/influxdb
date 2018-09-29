import {getSetupStatus, setSetupParams, SetupParams} from 'src/onboarding/apis'
import AJAX from 'src/utils/ajax'

jest.mock('src/utils/ajax', () => require('mocks/utils/ajax'))

describe('Onboarding.Apis', () => {
  afterEach(() => {
    jest.clearAllMocks()
  })

  describe('getSetupStatus', () => {
    it('is called with the expected body', () => {
      const url = '/api/v2/setup'
      getSetupStatus(url)
      expect(AJAX).toHaveBeenCalledWith({
        method: 'GET',
        url,
      })
    })
  })

  describe('setSetupParams', () => {
    it('is called with the expected body', () => {
      const url = '/api/v2/setup'
      const setupParams: SetupParams = {
        username: 'moo',
        password: 'pwoo',
        bucket: 'boo',
        org: 'ooo',
      }
      setSetupParams(url, setupParams)
      expect(AJAX).toHaveBeenCalledWith({
        method: 'POST',
        url,
        data: setupParams,
      })
    })
  })
})
