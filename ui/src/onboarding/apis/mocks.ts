import {
  getTelegrafConfigsResponse,
  authResponse,
  createTelegrafConfigResponse,
} from 'mocks/dummyData'

const telegrafsGet = jest.fn(() => Promise.resolve(getTelegrafConfigsResponse))
const telegrafsPost = jest.fn(() =>
  Promise.resolve(createTelegrafConfigResponse)
)
const telegrafsTelegrafIDPut = jest.fn(() =>
  Promise.resolve(createTelegrafConfigResponse)
)
const authorizationsGet = jest.fn(() => Promise.resolve(authResponse))
const setupPost = jest.fn(() => Promise.resolve())
const setupGet = jest.fn(() => Promise.resolve({data: {allowed: true}}))

export const telegrafsAPI = {
  telegrafsGet,
  telegrafsPost,
  telegrafsTelegrafIDPut,
}

export const setupAPI = {
  setupPost,
  setupGet,
}

export const authorizationsAPI = {
  authorizationsGet,
}
