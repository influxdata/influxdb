import {cleanup} from '@testing-library/react'
import 'intersection-observer'
import MutationObserver from 'mutation-observer'
// Adds MutationObserver as a polyfill for testing
window.MutationObserver = MutationObserver

jest.mock('honeybadger-js', () => () => null)

process.env.API_PREFIX = '/'
// cleans up state between @testing-library/react tests
afterEach(() => {
  cleanup()
})
