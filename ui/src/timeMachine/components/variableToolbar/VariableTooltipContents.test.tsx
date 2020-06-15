// Libraries
import React from 'react'
import {fireEvent} from '@testing-library/react'

// Components
import VariableTooltipContents from 'src/timeMachine/components/variableToolbar/VariableTooltipContents'

// Utils
import {renderWithRedux} from 'src/mockState'
import {AppState} from 'src/types'

const variableValues = {
  key1: 'value1',
  key2: 'value2',
}

const setInitialState = (state: AppState) => {
  return {
    ...state,
    resources: {
      variables: {
        status: 'Done',
        byID: {
          '04960e76e5afe000': {
            id: '04960e76e5afe000',
            orgID: '2e9f65b990c28374',
            name: 'example_map',
            description: '',
            selected: null,
            arguments: {
              type: 'map',
              values: variableValues,
            },
            createdAt: '2019-10-07T14:59:58.102045-07:00',
            updatedAt: '2019-10-07T14:59:58.102045-07:00',
            labels: [],
            links: {
              self: '/api/v2/variables/04960e76e5afe000',
              labels: '/api/v2/variables/04960e76e5afe000/labels',
              org: '/api/v2/orgs/2e9f65b990c28374',
            },
          },
          status: 'Done',
        },
        values: {
          de: {
            status: 'Done',
            values: {
              '04960e76e5afe000': {
                valueType: 'string',
                values: variableValues,
                selectedKey: 'key1',
                selectedValue: 'value1',
              },
            },
            order: ['04960e76e5afe000'],
          },
        },
      },
    },
  }
}

describe("Time Machine's variable dropdown", () => {
  describe('rendering map type variables', () => {
    it("renders the variables' keys, rather than their values", () => {
      const {getByTestId, getByText, getAllByText} = renderWithRedux(
        <VariableTooltipContents variableID="04960e76e5afe000" />,
        setInitialState
      )

      fireEvent.click(getByText('Value'))
      fireEvent.click(getByTestId('variable-dropdown--button'))
      Object.keys(variableValues).forEach(variableKey => {
        expect(getAllByText(variableKey)).toBeTruthy()
      })
    })
  })
})
