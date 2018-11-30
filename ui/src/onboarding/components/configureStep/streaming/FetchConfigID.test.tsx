// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import FetchConfigID from 'src/onboarding/components/configureStep/streaming/FetchConfigID'

jest.mock('src/utils/api', () => require('src/onboarding/apis/mocks'))

const setup = async (override = {}) => {
  const props = {
    org: 'default',
    children: jest.fn(),
    ...override,
  }

  const wrapper = await shallow(<FetchConfigID {...props} />)

  return {wrapper}
}

describe('FetchConfigID', () => {
  it('renders', async () => {
    const {wrapper} = await setup()
    expect(wrapper.exists()).toBe(true)
  })
})
