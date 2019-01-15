import {
  getSetupStatus,
  setSetupParams,
  SetupParams,
  getTelegrafConfigs,
  createTelegrafConfig,
  getAuthorizationToken,
} from 'src/onboarding/apis'

import {telegrafConfig, token} from 'mocks/dummyData'
import {
  telegrafsAPI,
  authorizationsAPI,
  setupAPI,
} from 'src/onboarding/apis/mocks'

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

  describe('getTelegrafConfigs', () => {
    it('should return an array of configs', async () => {
      const org = 'default'
      const result = await getTelegrafConfigs(org)

      expect(result).toEqual([telegrafConfig])
      expect(telegrafsAPI.telegrafsGet).toBeCalledWith(org)
    })
  })

  describe('createTelegrafConfig', () => {
    it('should return the newly created config', async () => {
      const result = await createTelegrafConfig(telegrafConfig)

      expect(result).toEqual(telegrafConfig)
    })
  })

  describe('getAuthorizationToken', () => {
    it('should return a token', async () => {
      const username = 'iris'
      const result = await getAuthorizationToken(username)

      expect(result).toEqual(token)
      expect(authorizationsAPI.authorizationsGet).toBeCalledWith(
        undefined,
        username
      )
    })
  })
})
