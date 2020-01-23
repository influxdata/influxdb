import {cleanup} from 'react-testing-library'
import 'intersection-observer'

jest.mock('honeybadger-js', () => () => null)

process.env.API_PREFIX = '/'
// cleans up state between react-testing-library tests
afterEach(() => {
  cleanup()
})
