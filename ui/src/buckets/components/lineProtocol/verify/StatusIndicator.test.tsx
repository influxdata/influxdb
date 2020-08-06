// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {StatusIndicator} from 'src/dataLoaders/components/lineProtocolWizard/verify/StatusIndicator'

// Types
import {RemoteDataState} from 'src/types'

const setup = (override?) => {
  const props = {
    status: RemoteDataState.NotStarted,
    ...override,
  }

  const wrapper = shallow(<StatusIndicator {...props} />)

  return {wrapper}
}

describe('StatusIndicator', () => {
  describe('rendering', () => {
    it('renders!', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })
})
