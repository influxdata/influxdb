import React from 'react'
import {shallow} from 'enzyme'

import {SourceForm} from 'src/sources/components/SourceForm'
import {me} from 'test/resources'

const setup = (override = {}) => {
  const noop = () => {}
  const props = {
    source: {
      url: '',
      name: '',
      username: '',
      password: '',
      telegraf: '',
      insecureSkipVerify: false,
      default: false,
      metaUrl: '',
    },
    editMode: false,
    onSubmit: noop,
    onInputChange: noop,
    onBlurSourceURL: noop,
    isUsingAuth: false,
    gotoPurgatory: noop,
    isInitialSource: false,
    me,
    ...override,
  }

  const wrapper = shallow(<SourceForm {...props} />)
  return {wrapper, props}
}

describe('Sources.Components.SourceForm', () => {
  describe('rendering', () => {
    it('renders inputs', () => {
      const {wrapper} = setup()
      const inputs = wrapper.find('input')

      expect(inputs.exists()).toBe(true)
    })
  })
})
