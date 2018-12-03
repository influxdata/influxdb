// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import VerifyDataSwitcher from 'src/onboarding/components/verifyStep/VerifyDataSwitcher'
import DataStreaming from 'src/onboarding/components/verifyStep/DataStreaming'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

const setup = (override = {}) => {
  const props = {
    type: DataLoaderType.Empty,
    org: '',
    username: '',
    bucket: '',
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

      const dataStreaming = wrapper.find(DataStreaming)
      expect(dataStreaming.exists()).toBe(true)
    })
  })
})
