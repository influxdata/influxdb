// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {CreateOrUpdateConfig} from 'src/dataLoaders/components/verifyStep/CreateOrUpdateConfig'

jest.mock('src/utils/api', () => require('src/onboarding/apis/mocks'))

const setup = async (override = {}) => {
  const props = {
    org: 'default',
    children: jest.fn(),
    onSaveTelegrafConfig: jest.fn(),
    createDashboardsForPlugins: jest.fn(),
    notify: jest.fn(),
    authToken: '',
    ...override,
  }

  const wrapper = await shallow(<CreateOrUpdateConfig {...props} />)

  return {wrapper}
}

describe('CreateOrUpdateConfig', () => {
  it('renders', async () => {
    const {wrapper} = await setup()
    expect(wrapper.exists()).toBe(true)
  })
})
