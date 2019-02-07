import {cleanup} from 'react-testing-library'

// cleans up state between react-testing-library tests
afterEach(() => {
  cleanup()
})
