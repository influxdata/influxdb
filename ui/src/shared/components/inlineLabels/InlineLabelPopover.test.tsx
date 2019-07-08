// Libraries
import React from 'react'
import {render, fireEvent} from 'react-testing-library'

// Components
import InlineLabelPopover from 'src/shared/components/inlineLabels/InlineLabelPopover'

// Constants
import {labels} from 'mocks/dummyData'
import {ADD_NEW_LABEL_ITEM_ID} from 'src/shared/components/inlineLabels/InlineLabelsEditor'

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

const setup = (override = {}) => {
  const props = {
    searchTerm: '',
    selectedItemID: labels[0].id,
    onUpdateSelectedItemID: jest.fn(),
    allLabelsUsed: false,
    onDismiss: jest.fn(),
    onStartCreatingLabel: jest.fn(),
    onInputChange: jest.fn(),
    filteredLabels,
    onAddLabel: jest.fn(),
    ...override,
  }

  return render(<InlineLabelPopover {...props} />)
}

describe('Shared.Components.InlineLabelPopover', () => {
  describe('rendering', () => {
    it('renders with text field in focus', () => {
      const {getByTestId} = setup()

      const input = getByTestId('inline-labels--popover-field')

      expect(document.activeElement).toEqual(input)
    })
  })

  describe('mouse interactions', () => {
    it('clicking outside the input refocuses it', () => {
      const {getByTestId} = setup()

      const input = getByTestId('inline-labels--popover-field')
      const popover = getByTestId('inline-labels--popover')
      fireEvent.click(popover)

      expect(document.activeElement).toEqual(input)
    })

    it('hovering over a list item updates the selected item ID', () => {
      const secondLabel = labels[1]
      const onUpdateSelectedItemID = jest.fn()
      const {getByTestId} = setup({onUpdateSelectedItemID})

      const secondListItem = getByTestId(`label-list--item ${secondLabel.name}`)
      fireEvent.mouseOver(secondListItem)

      expect(onUpdateSelectedItemID).toHaveBeenCalledWith(secondLabel.id)
    })

    it('clicking a list item adds a label and selects the next item on the list', () => {
      const secondLabel = labels[1]
      const onAddLabel = jest.fn()
      const {getByTestId} = setup({onAddLabel})

      const secondListItem = getByTestId(`label-list--item ${secondLabel.name}`)
      fireEvent.click(secondListItem)

      expect(onAddLabel).toHaveBeenCalledWith(secondLabel.id)
    })
  })

  describe('keyboard interactions', () => {
    it('typing in the input updates parent component', () => {
      const onInputChange = jest.fn(e => e.target.value)
      const {getByTestId} = setup({onInputChange})

      const testString = 'bananas'

      const input = getByTestId('inline-labels--popover-field')
      fireEvent.change(input, {target: {value: testString}})

      expect(onInputChange).toHaveReturnedWith(testString)
    })

    it('pressing ESCAPE dismisses the popover', () => {
      const onDismiss = jest.fn()
      const {getByTestId} = setup({onDismiss})

      const input = getByTestId('inline-labels--popover-field')
      fireEvent.keyDown(input, {key: 'Escape', code: 27})

      expect(onDismiss).toHaveBeenCalled()
    })

    it('pressing ENTER adds the selected item', () => {
      const selectedItemID = labels[0].id
      const onAddLabel = jest.fn()
      const {getByTestId} = setup({
        onAddLabel,
        selectedItemID,
      })

      const input = getByTestId('inline-labels--popover-field')
      fireEvent.keyDown(input, {key: 'Enter', code: 13})

      expect(onAddLabel).toHaveBeenCalledWith(selectedItemID)
    })

    it('typing a new label name and pressing ENTER starts label creation flow', () => {
      const onStartCreatingLabel = jest.fn()
      const searchTerm = 'swogglez'
      const selectedItemID = ADD_NEW_LABEL_ITEM_ID
      const {getByTestId} = setup({
        onStartCreatingLabel,
        searchTerm,
        selectedItemID,
      })

      const input = getByTestId('inline-labels--popover-field')
      fireEvent.keyDown(input, {key: 'Enter', code: 13})

      expect(onStartCreatingLabel).toHaveBeenCalled()
    })

    it('pressing ARROWUP selects the previous item', () => {
      const selectedItemID = filteredLabels[1].id
      const previousItemID = filteredLabels[0].id
      const onUpdateSelectedItemID = jest.fn()
      const {getByTestId} = setup({
        onUpdateSelectedItemID,
        selectedItemID,
      })

      const input = getByTestId('inline-labels--popover-field')
      fireEvent.keyDown(input, {key: 'ArrowUp', code: 38})

      expect(onUpdateSelectedItemID).toHaveBeenCalledWith(previousItemID)
    })

    it('pressing ARROWUP selects the same item', () => {
      const selectedItemID = filteredLabels[0].id
      const onUpdateSelectedItemID = jest.fn()
      const {getByTestId} = setup({
        onUpdateSelectedItemID,
        selectedItemID,
      })

      const input = getByTestId('inline-labels--popover-field')
      fireEvent.keyDown(input, {key: 'ArrowUp', code: 38})

      expect(onUpdateSelectedItemID).toHaveBeenCalledWith(selectedItemID)
    })

    it('pressing ARROWDOWN selects the next item', () => {
      const selectedItemID = filteredLabels[1].id
      const nextItemID = filteredLabels[2].id
      const onUpdateSelectedItemID = jest.fn()
      const {getByTestId} = setup({
        onUpdateSelectedItemID,
        selectedItemID,
      })

      const input = getByTestId('inline-labels--popover-field')
      fireEvent.keyDown(input, {key: 'ArrowDown', code: 40})

      expect(onUpdateSelectedItemID).toHaveBeenCalledWith(nextItemID)
    })

    it('pressing ARROWDOWN selects the same item', () => {
      const selectedItemID = filteredLabels[2].id
      const onUpdateSelectedItemID = jest.fn()
      const {getByTestId} = setup({
        onUpdateSelectedItemID,
        selectedItemID,
      })

      const input = getByTestId('inline-labels--popover-field')
      fireEvent.keyDown(input, {key: 'ArrowDown', code: 40})

      expect(onUpdateSelectedItemID).toHaveBeenCalledWith(selectedItemID)
    })
  })
})
