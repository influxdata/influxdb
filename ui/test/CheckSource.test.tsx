import React from 'react'
import {shallow} from 'enzyme'
import {CheckSources} from 'src/CheckSources'
import MockChild from 'mocks/MockChild'

import {source} from 'test/resources'

jest.mock('src/sources/apis', () => require('mocks/sources/apis'))
const getSources = jest.fn(() => Promise.resolve)

const setup = (override?) => {
  const props = {
    getSources,
    sources: [source],
    params: {
      sourceID: source.id,
    },
    router: {},
    location: {
      pathname: 'sources',
    },
    auth: {
      isUsingAuth: false,
      me: {},
    },
    notify: () => {},
    errorThrown: () => {},
    ...override,
  }

  const wrapper = shallow(
    <CheckSources {...props}>
      <MockChild />
    </CheckSources>
  )

  return {
    wrapper,
    props,
  }
}

describe('CheckSources', () => {
  describe('rendering', () => {
    it('renders', async () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })

    it('renders a spinner when the component is fetching', () => {
      const {wrapper} = setup()
      const spinner = wrapper.find('.page-spinner')

      expect(spinner.exists()).toBe(true)
    })

    it('renders its children when it is done fetching', done => {
      const {wrapper} = setup()

      // ensure that assertion runs after async behavior of getSources
      process.nextTick(() => {
        wrapper.update()
        const child = wrapper.find(MockChild)
        expect(child.exists()).toBe(true)
        done()
      })
    })
  })
})
