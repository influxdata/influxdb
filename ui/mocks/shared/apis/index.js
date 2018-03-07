import {kapacitor} from 'mocks/dummy'

export const getKapacitor = jest.fn(() => Promise.resolve(kapacitor))
