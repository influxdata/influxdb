import {
  getTelegrafConfigsResponse,
  authResponse,
  createTelegrafConfigResponse,
} from 'mocks/dummyData'

const telegrafsGet = jest.fn(() => Promise.resolve(getTelegrafConfigsResponse))
const telegrafsPost = jest.fn(() =>
  Promise.resolve(createTelegrafConfigResponse)
)
const authorizationsGet = jest.fn(() => Promise.resolve(authResponse))

export const telegrafsAPI = {
  telegrafsGet,
  telegrafsPost,
}

export const authorizationsAPI = {
  authorizationsGet,
}
