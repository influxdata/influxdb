// Libraries
import React from 'react'

// Components
import DataListening from 'src/dataLoaders/components/verifyStep/DataListening'

// Utils
import {renderWithRedux} from 'src/mockState'
import {fireEvent} from 'react-testing-library'

const setInitialState = state => {
  return {
    ...state,
    orgs: [
      {
        id: 'foo',
      },
    ],
  }
}

describe('Onboarding.Components.DataListening', () => {
  describe('if button is clicked', () => {
    it('displays connection information', () => {
      const {getByTitle, getByText} = renderWithRedux(
        <DataListening bucket="bucket" />,
        setInitialState
      )

      const button = getByTitle('Listen for Data')

      fireEvent.click(button)

      const message = getByText('Awaiting Connection...')

      expect(message).toBeDefined()
    })
  })
})
