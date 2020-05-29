// Libraries
import React from 'react'
import {act} from 'react-dom/test-utils'
import {fireEvent} from 'react-testing-library'
import 'intersection-observer'

// Components
import InlineLabelsEditor from 'src/shared/components/inlineLabels/InlineLabelsEditor'

// Constants
import {labels} from 'mocks/dummyData'
const selectedLabels = [labels[0]]

const filteredLabels = [
  ...labels,
  {
    id: '0003',
    name: 'Pineapple',
    properties: {
      color: '#FFB94A',
      description: 'Tangy and yellow',
    },
  },
]

import {renderWithRedux} from 'src/mockState'

const setup = (override = {}) => {
  const props = {
    selectedLabels,
    labels,
    onAddLabel: jest.fn(),
    onCreateLabel: jest.fn(),
    searchTerm: '',
    triggerRef: {current: null},
    selectedItemID: labels[0].id,
    onUpdateSelectedItemID: jest.fn(),
    allLabelsUsed: false,
    onStartCreatingLabel: jest.fn(),
    onInputChange: jest.fn(),
    filteredLabels,
    ...override,
  }

  return renderWithRedux(<InlineLabelsEditor {...props} />)
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
    it('hovering the plus button opens the popover', () => {
      const {getByTestId, getAllByTestId} = setup()

      const plusButton = getByTestId('inline-labels--add')

      act(() => {
        plusButton.click()
      })

      const popover = getAllByTestId('inline-labels--popover-field')

      expect(popover).toHaveLength(1)
    })

    // Maximum call stack exceeded
    it.skip('clicking the suggestion item shows create label overlay with the name field correctly populated', () => {
      const {getByTestId, getAllByTestId} = setup()

      const plusButton = getByTestId('inline-labels--add')
      act(() => {
        plusButton.click()
      })

      const inputValue = 'yodelling is rad'

      const input = getByTestId('inline-labels--popover-field')
      fireEvent.change(input, {target: {value: inputValue}})

      const suggestionItem = getByTestId('inline-labels--create-new')
      // Maximum call stack exceeded on this line
      fireEvent.click(suggestionItem)

      const labelOverlayForm = getAllByTestId('label-overlay-form')

      expect(labelOverlayForm).toHaveLength(1)

      const labelOverlayNameField = getByTestId('create-label-form--name')

      expect(labelOverlayNameField.getAttribute('value')).toEqual(inputValue)
    })

    it('clicking a list item adds a label and selects the next item on the list', () => {
      const secondLabel = labels[1]
      const onAddLabel = jest.fn()
      const {getByTestId} = setup({onAddLabel})
      const button = getByTestId('inline-labels--add')
      act(() => {
        button.click()
      })
      const secondListItem = getByTestId(`label-list--item ${secondLabel.name}`)
      fireEvent.click(secondListItem)

      expect(onAddLabel).toHaveBeenCalledWith(secondLabel)
    })
  })
})
