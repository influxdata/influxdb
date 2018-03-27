jest.mock('src/utils/ajax', () => require('mocks/utils/ajax'))

export const getSuggestions = jest.fn(() => Promise.resolve([])
