// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {VerifyDataStep} from 'src/onboarding/components/verifyStep/VerifyDataStep'
import VerifyDataSwitcher from 'src/onboarding/components/verifyStep/VerifyDataSwitcher'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

// Constants
import {defaultOnboardingStepProps} from 'mocks/dummyData'
import {RemoteDataState} from 'src/types'

jest.mock('src/utils/api', () => require('src/onboarding/apis/mocks'))

const setup = (override = {}) => {
  const props = {
    ...defaultOnboardingStepProps,
    type: DataLoaderType.Empty,
    telegrafPlugins: [],
    stepIndex: 2,
    substep: 0,
    telegrafConfigID: '',
    onSaveTelegrafConfig: jest.fn(),
    onSetActiveTelegrafPlugin: jest.fn(),
    onSetPluginConfiguration: jest.fn(),
    lpStatus: RemoteDataState.NotStarted,
    bucket: 'defbuck',
    username: 'user',
    org: '',
    notify: jest.fn(),
    selectedBucket: '',
    ...override,
  }

  const wrapper = shallow(<VerifyDataStep {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.VerifyStep.VerifyDataStep', () => {
  it('renders', () => {
    const {wrapper} = setup()
    const switcher = wrapper.find(VerifyDataSwitcher)

    expect(wrapper.exists()).toBe(true)
    expect(switcher.exists()).toBe(true)
  })
})
