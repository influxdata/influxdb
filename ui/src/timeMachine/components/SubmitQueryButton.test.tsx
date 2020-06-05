// Libraries
import React from 'react'

// Components
import SubmitQueryButton from 'src/timeMachine/components/SubmitQueryButton'

// Utils
import {renderWithRedux} from 'src/mockState'
import {fireEvent, waitFor} from '@testing-library/react'

// Types
import {RemoteDataState} from 'src/types'

const stateOverride = {
  timeMachines: {
    activeTimeMachineID: 'veo',
    timeMachines: {
      veo: {
        draftQueries: [{text: 'this is a draft query'}],
        activeQueryIndex: 0,
        queryResults: {
          status: RemoteDataState.NotStarted,
        },
        view: {
          properties: {
            queries: [{text: 'draftstate'}],
          },
        },
      },
    },
  },
}

describe('TimeMachine.Components.SubmitQueryButton', () => {
  describe('if button is clicked', () => {
    it('disables the submit button when no query is present', () => {
      const {getByTitle} = renderWithRedux(<SubmitQueryButton />)

      const SubmitBtn = getByTitle('Submit')
      fireEvent.click(SubmitBtn)
      // expect the button to still be on submit
      expect(getByTitle('Submit')).toBeTruthy()
    })
    it('allows the query to be cancelled after submission', async () => {
      const {getByTitle} = renderWithRedux(<SubmitQueryButton />, s => ({
        ...s,
        ...stateOverride,
      }))

      const SubmitBtn = getByTitle('Submit')
      fireEvent.click(SubmitBtn)

      const CancelBtn = getByTitle('Cancel')
      // expect the button to toggle to Cancel
      expect(CancelBtn).toBeTruthy()
      fireEvent.click(CancelBtn)
      // aborts the query and returns the query to submit mode
      expect(await waitFor(() => getByTitle('Submit'))).toBeTruthy()
    })
  })
})
