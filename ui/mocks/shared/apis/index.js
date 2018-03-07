import {kapacitor} from 'mocks/dummy'

export const getKapacitor = jest.fn(() => Promise.resolve(kapacitor))
export const pingKapacitor = jest.fn(() => Promise.resolve())
