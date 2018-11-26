import {
  getSetupStatus,
  setSetupParams,
  SetupParams,
  getTelegrafConfigs,
  getAuthorizationToken,
} from 'src/onboarding/apis'

import AJAX from 'src/utils/ajax'

import {telegrafConfig, token} from 'src/onboarding/resources'
import {telegrafsAPI, authorizationsAPI} from 'src/onboarding/apis/mocks'

jest.mock('src/utils/ajax', () => require('mocks/utils/ajax'))
jest.mock('src/utils/api', () => require('src/onboarding/apis/mocks'))

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

  describe('getTelegrafConfigs', () => {
    it('should return an array of configs', async () => {
      const org = 'default'
      const result = await getTelegrafConfigs(org)

      expect(result).toEqual([telegrafConfig])
      expect(telegrafsAPI.telegrafsGet).toBeCalledWith(org)
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
