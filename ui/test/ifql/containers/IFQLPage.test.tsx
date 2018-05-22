import React from 'react'
import {shallow} from 'enzyme'

import {IFQLPage} from 'src/ifql/containers/IFQLPage'
import TimeMachine from 'src/ifql/components/TimeMachine'
import {ActionTypes} from 'src/ifql/actions'

jest.mock('src/ifql/apis', () => require('mocks/ifql/apis'))

const setup = () => {
  const props = {
    links: {
      self: '',
      suggestions: '',
      ast: '',
    },
    services: [],
    sources: [],
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
  }

  const wrapper = shallow(<IFQLPage {...props} />)

  return {
    wrapper,
  }
}

describe('IFQL.Containers.IFQLPage', () => {
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
