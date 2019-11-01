// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import ConnectionInformation, {
  LoadingState,
} from 'src/dataLoaders/components/verifyStep/ConnectionInformation'

// Types

const setup = (override = {}) => {
  const props = {
    loading: LoadingState.NotStarted,
    bucket: 'defbuck',
    countDownSeconds: 60,
    ...override,
  }

  const wrapper = shallow(<ConnectionInformation {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.ConnectionInformation', () => {
  it('renders', () => {
    const {wrapper} = setup()

    expect(wrapper.exists()).toBe(true)
  })
})
