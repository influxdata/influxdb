import {queryConfig} from 'mocks/dummy'

export const getQueryConfigAndStatus = jest.fn(() =>
  Promise.resolve({data: queryConfig})
)
