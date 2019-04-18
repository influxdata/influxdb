// Libraries
import React from 'react'
import {renderWithRedux} from 'src/mockState'

// Components
import CollectorList from 'src/telegrafs/components/CollectorList'

// Constants
import {telegraf} from 'mocks/dummyData'

const setup = (override?) => {
  const props = {
    collectors: telegraf,
    emptyState: <></>,
    onDelete: jest.fn(),
    onUpdate: jest.fn(),
    onOpenInstructions: jest.fn(),
    onOpenTelegrafConfig: jest.fn(),
    ...override,
  }

  const wrapper = renderWithRedux(<CollectorList {...props} />)

  return {wrapper}
}

describe('CollectorList', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      expect(wrapper).toMatchSnapshot()
    })
  })
})
