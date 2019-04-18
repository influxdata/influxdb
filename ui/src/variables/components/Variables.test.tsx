// Libraries
import React from 'react'
import {renderWithRedux} from 'src/mockState'

// Components
import VariableList from 'src/variables/components/VariableList'

// Constants
import {variables} from 'mocks/dummyData'

const setup = (override?) => {
  const props = {
    variables,
    emptyState: <></>,
    onDeleteVariable: jest.fn(),
    ...override,
  }

  const wrapper = renderWithRedux(<VariableList {...props} />)

  return {wrapper}
}

describe('VariableList', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      expect(wrapper).toMatchSnapshot()
    })
  })
})
