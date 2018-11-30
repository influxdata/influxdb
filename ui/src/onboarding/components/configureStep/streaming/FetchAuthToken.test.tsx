// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import FetchAuthToken from 'src/onboarding/components/configureStep/streaming/FetchAuthToken'

jest.mock('src/utils/api', () => require('src/onboarding/apis/mocks'))

const setup = async (override = {}) => {
  const props = {
    bucket: '',
    username: '',
    children: jest.fn(),
    ...override,
  }

  const wrapper = await shallow(<FetchAuthToken {...props} />)

  return {wrapper}
}

describe('FetchAuthToken', () => {
  it('renders', async () => {
    const {wrapper} = await setup()
    expect(wrapper.exists()).toBe(true)
  })
})
