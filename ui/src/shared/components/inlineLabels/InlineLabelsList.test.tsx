// Libraries
import React from 'react'
import {render, fireEvent} from 'react-testing-library'

// Components
import InlineLabelsList from 'src/shared/components/inlineLabels/InlineLabelsList'

// Constants
import {labels} from 'mocks/dummyData'
import {ADD_NEW_LABEL_LABEL} from 'src/shared/components/inlineLabels/InlineLabelsEditor'

const setup = (override = {}) => {
  const props = {
    searchTerm: '',
    selectedItemID: '',
    onUpdateSelectedItemID: jest.fn(),
    filteredLabels: labels,
    onItemClick: jest.fn(),
    allLabelsUsed: false,
    onStartCreatingLabel: jest.fn(),
    ...override,
  }

  return render(<InlineLabelsList {...props} />)
}

describe('Shared.Components.InlineLabelsList', () => {
  describe('rendering', () => {
    it('renders 1 label component per filtered label', () => {
      const {getAllByTestId} = setup()

      const labelElements = getAllByTestId(/label--pill\s/)

      expect(labelElements).toHaveLength(labels.length)
    })

    it('renders correct empty state when all labels are used', () => {
      const {getAllByTestId} = setup({filteredLabels: [], allLabelsUsed: true})

      const noLabelsIndicator = getAllByTestId('inline-labels-list--used-all')

      expect(noLabelsIndicator).toHaveLength(1)
    })

    it('renders correct empty state when no labels exist', () => {
      const {getAllByTestId} = setup({filteredLabels: [], searchTerm: null})

      const noLabelsIndicator = getAllByTestId('inline-labels-list--none-exist')

      expect(noLabelsIndicator).toHaveLength(1)
    })

    it('renders a suggestion item when present in labels list', () => {
      const {getAllByTestId} = setup({
        filteredLabels: [ADD_NEW_LABEL_LABEL, ...labels],
      })

      const suggestionElement = getAllByTestId('inline-labels--create-new')

      expect(suggestionElement).toHaveLength(1)
    })
  })

  describe('mouse interactions', () => {
    it('clicking a list item fires the correct function', () => {
      const firstLabel = labels[0]
      const onItemClick = jest.fn()
      const {getByTestId} = setup({onItemClick})

      const firstListItem = getByTestId(`label--pill ${firstLabel.name}`)
      fireEvent.click(firstListItem)

      expect(onItemClick).toHaveBeenCalledWith(firstLabel.id)
    })

    it('mousing over a list item fires the correct function', () => {
      const firstLabel = labels[0]
      const onUpdateSelectedItemID = jest.fn()
      const {getByTestId} = setup({onUpdateSelectedItemID})

      const firstListItem = getByTestId(`label--pill ${firstLabel.name}`)
      fireEvent.mouseOver(firstListItem)

      expect(onUpdateSelectedItemID).toHaveBeenCalledWith(firstLabel.id)
    })
  })
})
