// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import TaskForm from 'src/tasks/components/TaskForm'
import {Form} from 'src/clockface'

// Constants
import defaultTaskOptions from 'src/tasks/reducers/v2'

const setup = (override?) => {
  const props = {
    orgs: [],
    taskOptions: defaultTaskOptions,
    onChangeScheduleType: jest.fn(),
    onChangeInput: jest.fn(),
    onChangeTaskOrgID: jest.fn(),
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
      expect(wrapper).toMatchSnapshot()
    })
  })
})
