jest.mock('src/utils/ajax', () => require('mocks/utils/ajax'))

export const getSuggestions = jest.fn(() => Promise.resolve([]))
export const getAST = jest.fn(() => Promise.resolve({}))
const showDatabasesResponse = {
  data: {
    results: [{series: [{columns: ['name'], values: [['mydb1'], ['mydb2']]}]}],
  },
}
export const showDatabases = jest.fn(() =>
  Promise.resolve(showDatabasesResponse)
)
export const getTimeSeries = jest.fn(() => Promise.resolve({data: ''}))
