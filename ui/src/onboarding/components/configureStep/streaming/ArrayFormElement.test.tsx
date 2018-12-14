// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import ArrayFormElement from 'src/onboarding/components/configureStep/streaming/ArrayFormElement'
import {FormElement} from 'src/clockface'
import TagInput from 'src/shared/components/TagInput'

const setup = (override = {}) => {
  const props = {
    fieldName: '',
    addTagValue: jest.fn(),
    removeTagValue: jest.fn(),
    autoFocus: true,
    value: [],
    helpText: '',
    ...override,
  }

  const wrapper = shallow(<ArrayFormElement {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.ConfigureStep.Streaming.ArrayFormElement', () => {
  it('renders', () => {
    const fieldName = 'yo'
    const {wrapper} = setup({fieldName})
    const formElement = wrapper.find(FormElement)
    const tagInput = wrapper.find(TagInput)

    expect(wrapper.exists()).toBe(true)
    expect(formElement.exists()).toBe(true)
    expect(tagInput.exists()).toBe(true)
  })
})
