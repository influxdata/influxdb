// Libraries
import React from 'react'
import {renderWithRedux} from 'src/mockState'

// Components
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'

// Constants
import {labels} from 'mocks/dummyData'
import {RemoteDataState} from 'src/types'

const l1 = labels[0]
const selectedLabelIDs = [labels[0].id]
const appState = {
  resources: {
    labels: {
      byID: {
        [l1.id]: l1,
      },
      allIDs: [l1.id],
      status: RemoteDataState.Done,
    },
  },
}

const setup = (override = {}, stateOverride = appState) => {
  const props = {
    selectedLabelIDs,
    labels,
    onRemoveLabel: jest.fn(),
    onAddLabel: jest.fn(),
    onCreateLabel: jest.fn(),
    onFilterChange: jest.fn(),
    ...override,
  }

  return renderWithRedux(<InlineLabels {...props} />, s => ({
    ...s,
    ...stateOverride,
  }))
}

describe('Shared.Components.InlineLabels', () => {
  describe('rendering', () => {
    it('renders selected labels', () => {
      const {getAllByTestId} = setup()

      const selected = getAllByTestId(/label--pill\s/)

      expect(selected).toHaveLength(selectedLabelIDs.length)
    })
  })

  describe('mouse interactions', () => {
    it('clicking a label sets its name as a search term', () => {
      const onFilterChange = jest.fn()
      const wrapper = setup({onFilterChange})
      const firstSelectedLabelName = l1.name

      const label = wrapper.getByTestId(`label--pill ${firstSelectedLabelName}`)
      label.click()

      expect(onFilterChange).toHaveBeenCalledWith(firstSelectedLabelName)
    })

    it('clicking a label X button fires the delete function', () => {
      const onRemoveLabel = jest.fn()
      const wrapper = setup({onRemoveLabel})
      const firstSelectedLabel = l1

      const labelX = wrapper.getByTestId(
        `label--pill--delete ${firstSelectedLabel.name}`
      )
      labelX.click()

      expect(onRemoveLabel).toHaveBeenCalledWith(firstSelectedLabel)
    })
  })
})
