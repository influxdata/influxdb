// Libraries
import React from 'react'
import {render, fireEvent} from 'react-testing-library'

// Components
import InlineLabelsEditor from 'src/shared/components/inlineLabels/InlineLabelsEditor'

// Constants
import {labels} from 'mocks/dummyData'
const selectedLabels = [labels[0]]

const setup = (override = {}) => {
  const props = {
    selectedLabels,
    labels,
    onAddLabel: jest.fn(),
    onCreateLabel: jest.fn(),
    ...override,
  }

  return render(<InlineLabelsEditor {...props} />)
}

describe('Shared.Components.InlineLabelsEditor', () => {
  describe('rendering', () => {
    it('renders a plus button', () => {
      const {getAllByTestId} = setup()

      const plusButton = getAllByTestId('inline-labels--add')

      expect(plusButton).toHaveLength(selectedLabels.length)
    })

    it('renders empty state with no selected labels', () => {
      const {getAllByTestId} = setup({selectedLabels: []})

      const noLabelsIndicator = getAllByTestId('inline-labels--empty')

      expect(noLabelsIndicator).toHaveLength(1)
    })
  })

  describe('mouse interactions', () => {
    it('clicking the plus button opens the popover', () => {
      const {getByTestId, getAllByTestId} = setup()

      const plusButton = getByTestId('inline-labels--add')
      plusButton.click()

      const popover = getAllByTestId('inline-labels--popover')

      expect(popover).toHaveLength(1)
    })

    it('clicking the suggestion item shows create label overlay with the name field correctly populated', () => {
      const {getByTestId, getAllByTestId} = setup()

      const plusButton = getByTestId('inline-labels--add')
      plusButton.click()

      const inputValue = 'yodelling is rad'

      const input = getByTestId('inline-labels--popover-field')
      fireEvent.change(input, {target: {value: inputValue}})

      const suggestionItem = getByTestId('inline-labels--create-new')
      fireEvent.click(suggestionItem)

      const labelOverlayForm = getAllByTestId('label-overlay-form')

      expect(labelOverlayForm).toHaveLength(1)

      const labelOverlayNameField = getByTestId('create-label-form--name')

      expect(labelOverlayNameField.getAttribute('value')).toEqual(inputValue)
    })
  })
})
