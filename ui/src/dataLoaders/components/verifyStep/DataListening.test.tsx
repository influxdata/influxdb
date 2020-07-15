// Libraries
import React from 'react'
import fetchMock from 'jest-fetch-mock'
fetchMock.enableMocks()

// Components
import DataListening from 'src/dataLoaders/components/verifyStep/DataListening'

// Utils
import {renderWithReduxAndRouter} from 'src/mockState'
import {fireEvent} from '@testing-library/react'

describe('Onboarding.Components.DataListening', () => {
  describe('if button is clicked', () => {
    it('displays connection information', () => {
      const {getByTitle, getByText} = renderWithReduxAndRouter(
        <DataListening bucket="bucket" params={{orgID: 'org123'}} />
      )

      const button = getByTitle('Listen for Data')

      fireEvent.click(button)

      const message = getByText('Awaiting Connection...')

      expect(message).toBeDefined()
    })
  })
})
