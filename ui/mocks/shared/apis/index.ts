import {queryConfig} from 'mocks/dummyData'

export const getQueryConfigAndStatus = jest.fn(() =>
  Promise.resolve({data: queryConfig})
)
