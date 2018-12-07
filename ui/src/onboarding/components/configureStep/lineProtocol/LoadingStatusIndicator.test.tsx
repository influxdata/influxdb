// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import LoadingStatusIndicator from 'src/onboarding/components/configureStep/lineProtocol/LoadingStatusIndicator'

// Types
import {RemoteDataState} from 'src/types'

const setup = (override?) => {
  const props = {
    status: RemoteDataState.NotStarted,
    ...override,
  }

  const wrapper = shallow(<LoadingStatusIndicator {...props} />)

  return {wrapper}
}

describe('LoadingStatusIndicator', () => {
  describe('rendering', () => {
    it('renders!', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)

      expect(wrapper).toMatchSnapshot()
    })
  })
})
