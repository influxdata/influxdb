// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import FetchAuthToken from 'src/dataLoaders/components/verifyStep/FetchAuthToken'

jest.mock('src/utils/api', () => require('src/onboarding/apis/mocks'))

const setup = (override = {}) => {
  const props = {
    bucket: '',
    username: '',
    children: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<FetchAuthToken {...props} />)

  return {wrapper}
}

describe('FetchAuthToken', () => {
  it('renders', () => {
    const {wrapper} = setup()
    expect(wrapper.exists()).toBe(true)
  })
})
