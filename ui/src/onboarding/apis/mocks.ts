import {
  getTelegrafConfigsResponse,
  createTelegrafConfigResponse,
  setSetupParamsResponse,
} from 'mocks/dummyData'

const telegrafsGet = jest.fn(() =>
  Promise.resolve(getTelegrafConfigsResponse.data)
)
const telegrafsPost = jest.fn(() =>
  Promise.resolve(createTelegrafConfigResponse.data)
)
const telegrafsTelegrafIDPut = jest.fn(() =>
  Promise.resolve(createTelegrafConfigResponse.data)
)
const setupPost = jest.fn(() => Promise.resolve(setSetupParamsResponse))
const setupGet = jest.fn(() => Promise.resolve({data: {allowed: true}}))

export const telegrafsAPI = {
  telegrafsGet,
  telegrafsPost,
  telegrafsTelegrafIDPut,
}

export const client = {
  telegrafConfigs: {
    getAll: telegrafsGet,
    getAllByOrg: telegrafsGet,
    create: telegrafsPost,
  },
}

export const setupAPI = {
  setupPost,
  setupGet,
}
