// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {VerifyDataSwitcher} from 'src/onboarding/components/verifyStep/VerifyDataSwitcher'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'
import {RemoteDataState} from 'src/types'

const setup = (override = {}) => {
  const props = {
    notify: jest.fn(),
    type: DataLoaderType.Empty,
    org: '',
    username: '',
    bucket: '',
    authToken: '',
    telegrafConfigID: '',
    onSaveTelegrafConfig: jest.fn(),
    stepIndex: 4,
    onSetStepStatus: jest.fn(),
    onDecrementCurrentStep: jest.fn(),
    lpStatus: RemoteDataState.NotStarted,
    ...override,
  }

  const wrapper = shallow(<VerifyDataSwitcher {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.VerifyStep.VerifyDataSwitcher', () => {
  it('renders', () => {
    const {wrapper} = setup()

    expect(wrapper.exists()).toBe(true)
  })

  describe('If data type is streaming', () => {
    it('renders the DataStreaming component', () => {
      const {wrapper} = setup({type: DataLoaderType.Streaming})

      expect(wrapper).toMatchSnapshot()
    })
  })
})
