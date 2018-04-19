jest.mock('src/utils/ajax', () => require('mocks/utils/ajax'))

export const getSuggestions = jest.fn(() => Promise.resolve([]))
export const getAST = jest.fn(() => Promise.resolve({}))
export const getDatabases = jest.fn(() =>
  Promise.resolve(['db1', 'db2', 'db3'])
)
