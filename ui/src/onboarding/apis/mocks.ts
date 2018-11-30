import {telegrafConfigsResponse, authResponse} from 'src/onboarding/resources'

const telegrafsGet = jest.fn(() => Promise.resolve(telegrafConfigsResponse))
const authorizationsGet = jest.fn(() => Promise.resolve(authResponse))

export const telegrafsAPI = {
  telegrafsGet,
}

export const authorizationsAPI = {
  authorizationsGet,
}
