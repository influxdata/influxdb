import {cleanup} from 'react-testing-library'

process.env.API_PREFIX = '/'
// cleans up state between react-testing-library tests
afterEach(() => {
  cleanup()
})
