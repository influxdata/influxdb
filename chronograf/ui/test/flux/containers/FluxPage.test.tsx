import React from 'react'
import {shallow} from 'enzyme'

import {FluxPage} from 'src/flux/containers/FluxPage'
import TimeMachine from 'src/flux/components/TimeMachine'
import {ActionTypes} from 'src/flux/actions'
import {source} from 'test/resources/v2'

jest.mock('src/flux/apis', () => require('mocks/flux/apis'))

const setup = () => {
  const props = {
    links: {
      self: '',
      suggestions: '',
      ast: '',
    },
    services: [],
    source,
    script: '',
    notify: () => {},
    params: {
      sourceID: '',
    },
    updateScript: (script: string) => {
      return {
        type: ActionTypes.UpdateScript,
        payload: {
          script,
        },
      }
    },
    onGoToEditFlux: () => {},
  }

  const wrapper = shallow(<FluxPage {...props} />)

  return {
    wrapper,
  }
}

describe('Flux.Containers.FluxPage', () => {
  afterEach(() => {
    jest.clearAllMocks()
  })

  describe('rendering', () => {
    it('renders the page', async () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })

    it('renders the <TimeMachine/>', () => {
      const {wrapper} = setup()

      const timeMachine = wrapper.find(TimeMachine)

      expect(timeMachine.exists()).toBe(true)
    })
  })
})
