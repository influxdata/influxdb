// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {Form} from '@influxdata/clockface'
import TaskForm from 'src/tasks/components/TaskForm'

// Constants
import defaultTaskOptions from 'src/tasks/reducers'

const setup = (override?) => {
  const props = {
    orgs: [],
    taskOptions: defaultTaskOptions,
    onChangeScheduleType: jest.fn(),
    onChangeInput: jest.fn(),
    onChangeTaskOrgID: jest.fn(),
    tokens: [],
    selectedToken: '',
    onTokenChange: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<TaskForm {...props} />)

  return {wrapper}
}

describe('TaskForm', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      const form = wrapper.find(Form)

      expect(wrapper.exists()).toBe(true)
      expect(form.exists()).toBe(true)
    })
  })
})
