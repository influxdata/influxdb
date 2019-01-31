import {getSetupStatus, setSetupParams, SetupParams} from 'src/onboarding/apis'

import {setupAPI} from 'src/onboarding/apis/mocks'

jest.mock('src/utils/api', () => require('src/onboarding/apis/mocks'))

describe('Onboarding.Apis', () => {
  afterEach(() => {
    jest.clearAllMocks()
  })

  describe('getSetupStatus', () => {
    it('is called with the expected body', () => {
      getSetupStatus()
      expect(setupAPI.setupGet).toHaveBeenCalled()
    })
  })

  describe('setSetupParams', () => {
    it('is called with the expected body', () => {
      const setupParams: SetupParams = {
        username: 'moo',
        password: 'pwoo',
        bucket: 'boo',
        org: 'ooo',
      }
      setSetupParams(setupParams)
      expect(setupAPI.setupPost).toHaveBeenCalledWith(setupParams)
    })
  })
})
