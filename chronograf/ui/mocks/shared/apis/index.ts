import {kapacitor, queryConfig} from 'mocks/dummy'

export const getKapacitor = jest.fn(() => Promise.resolve(kapacitor))
export const getActiveKapacitor = jest.fn(() => Promise.resolve(kapacitor))
export const createKapacitor = jest.fn(() => Promise.resolve({data: kapacitor}))
export const updateKapacitor = jest.fn(() => Promise.resolve({data: kapacitor}))
export const pingKapacitor = jest.fn(() => Promise.resolve())
export const getQueryConfigAndStatus = jest.fn(() =>
  Promise.resolve({data: queryConfig})
)
