// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {Input} from '@influxdata/clockface'
import URIFormElement from 'src/shared/components/URIFormElement'

const setup = (override = {}) => {
  const props = {
    name: '',
    value: '',
    onChange: jest.fn(),
    helpText: '',
    ...override,
  }

  const wrapper = shallow(<URIFormElement {...props} />)

  return {wrapper}
}

describe('URIFormElement', () => {
  const {wrapper} = setup()

  it('renders', () => {
    const input = wrapper.find(Input)

    expect(wrapper.exists()).toBe(true)
    expect(input.exists()).toBe(true)
  })
})
