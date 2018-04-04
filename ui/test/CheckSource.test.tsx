import React from 'react'
import {mount} from 'enzyme'
import {CheckSources} from 'src/CheckSources'
import MockChild from 'mocks/MockChild'

import {source, me} from 'test/resources'

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

  const wrapper = mount(
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

    it('renders its children when it is done fetching', async () => {
      const {wrapper} = setup()

      // must call and await componentWillMount manually test to register state change
      await wrapper.instance().componentWillMount()
      wrapper.update()

      const kid = wrapper.find(MockChild)
      expect(kid.exists()).toBe(true)
    })
  })
})
