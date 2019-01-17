// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {ConfigureDataSourceStep} from 'src/onboarding/components/configureStep/ConfigureDataSourceStep'
import ConfigureDataSourceSwitcher from 'src/onboarding/components/configureStep/ConfigureDataSourceSwitcher'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

// Dummy Data
import {defaultOnboardingStepProps} from 'mocks/dummyData'

const setup = (override = {}) => {
  const props = {
    ...defaultOnboardingStepProps,
    telegrafPlugins: [],
    onSetActiveTelegrafPlugin: jest.fn(),
    onUpdateTelegrafPluginConfig: jest.fn(),
    onSetPluginConfiguration: jest.fn(),
    type: DataLoaderType.Empty,
    onAddConfigValue: jest.fn(),
    onRemoveConfigValue: jest.fn(),
    onSaveTelegrafConfig: jest.fn(),
    authToken: '',
    currentStepIndex: 3,
    substep: 0,
    location: null,
    router: null,
    routes: [],
    onSetConfigArrayValue: jest.fn(),
    bucket: '',
    org: '',
    username: '',
    buckets: [],
    ...override,
  }

  return shallow(<ConfigureDataSourceStep {...props} />)
}

describe('Onboarding.Components.ConfigureStep.ConfigureDataSourceStep', () => {
  it('renders switcher and buttons', async () => {
    const wrapper = setup()
    const switcher = wrapper.find(ConfigureDataSourceSwitcher)

    expect(wrapper.exists()).toBe(true)
    expect(switcher.exists()).toBe(true)
  })
})
