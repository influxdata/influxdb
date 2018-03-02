import React from 'react'
import {Dropdown} from 'shared/components/Dropdown'
import DropdownMenu, {DropdownMenuEmpty} from 'shared/components/DropdownMenu'
import DropdownHead from 'shared/components/DropdownHead'
import DropdownInput from 'shared/components/DropdownInput'

import {mount} from 'enzyme'

// mock props
const itemOne = {text: 'foo'}
const itemTwo = {text: 'bar'}
const items = [itemOne, itemTwo]

// automate shallow render and providing new props
const setup = (override = {}) => {
  const props = {
    items: [],
    selected: '',
    onChoose: jest.fn(),
    ...override,
  }

  const defaultState = {
    isOpen: false,
    searchTerm: '',
    filteredItems: items,
    highlightedItemIndex: null,
  }

  const dropdown = mount(<Dropdown {...props} />)

  return {
    props,
    dropdown,
    defaultState,
  }
}

describe('Components.Shared.Dropdown', () => {
  describe('rendering', () => {
    describe('initial render', () => {
      it('renders the <Dropdown/> button', () => {
        const {dropdown} = setup()

        expect(dropdown.exists()).toBe(true)
      })

      it('does not show the <DropdownMenu/> list', () => {
        const {dropdown} = setup({items})

        const menu = dropdown.find(DropdownMenu)
        expect(menu.exists()).toBe(false)
      })
    })

    describe('the <DropdownHead />', () => {
      const {dropdown} = setup()
      const head = dropdown.find(DropdownHead)

      expect(head.exists()).toBe(true)
    })

    describe('when there are no items in the dropdown', () => {
      it('renders the <DropdownMenuEmpty/> component', () => {
        const {dropdown} = setup()
        const empty = dropdown.find(DropdownMenuEmpty)

        expect(empty.exists()).toBe(true)
      })
    })

    describe('the <DropdownInput/>', () => {
      it('does not display the input by default', () => {
        const {dropdown} = setup()
        const input = dropdown.find(DropdownInput)

        expect(input.exists()).toBe(false)
      })

      it('displays the input when provided useAutoCompelete is true', () => {
        const {dropdown} = setup({items, useAutoComplete: true})
        let input = dropdown.find(DropdownInput)

        expect(input.exists()).toBe(false)

        dropdown.simulate('click')
        input = dropdown.find(DropdownInput)

        expect(input.exists()).toBe(true)
      })
    })

    describe('user interactions', () => {
      describe('opening the <DropdownMenu/>', () => {
        it('shows the menu when clicked', () => {
          const {dropdown} = setup({items})

          dropdown.simulate('click')

          const menu = dropdown.find(DropdownMenu)
          expect(dropdown.state().isOpen).toBe(true)
          expect(menu.exists()).toBe(true)
        })

        it('hides the menu when clicked twice', () => {
          const {dropdown} = setup({items})

          // first click
          dropdown.simulate('click')
          // second click
          dropdown.simulate('click')

          const menu = dropdown.find(DropdownMenu)
          expect(dropdown.state().isOpen).toBe(false)
          expect(menu.exists()).toBe(false)
        })
      })
    })
  })

  describe('instance methods', () => {
    describe('applyFilter', () => {
      it('filters the list by the searchTerm', () => {
        const {dropdown} = setup({items, useAutoComplete: true})

        dropdown.instance().applyFilter('fo')
        expect(dropdown.state().filteredItems).toEqual([{text: 'foo'}])
      })
    })

    describe('handleFilterChange', () => {
      it('resets filteredList and searchTerm if the filter is empty', () => {
        const {dropdown} = setup({items, useAutoComplete: true})
        const event = {target: {value: ''}}
        dropdown.instance().applyFilter('fo')

        // assert that the list is filtered
        expect(dropdown.state().filteredItems).toEqual([{text: 'foo'}])

        dropdown.instance().handleFilterChange(event)
        const {filteredItems, searchTerm} = dropdown.state()

        expect(filteredItems).toEqual(items)
        expect(searchTerm).toEqual('')
      })
    })

    describe('handleClickOutside', () => {
      it('sets isOpen to false', () => {
        const {dropdown} = setup()
        dropdown.simulate('click')
        dropdown.instance().handleClickOutside()

        expect(dropdown.state().isOpen).toBe(false)
      })
    })

    describe('handleClick', () => {
      it('fires the onClick prop', () => {
        const onClick = jest.fn()
        const {dropdown} = setup({onClick})
        dropdown.instance().handleClick()

        expect(onClick).toHaveBeenCalled()
      })

      it('toggles the isOpen', () => {
        const {dropdown} = setup()

        dropdown.instance().handleClick()
        expect(dropdown.state().isOpen).toBe(true)

        dropdown.instance().handleClick()
        expect(dropdown.state().isOpen).toBe(false)
      })

      it('does nothing if disabled', () => {
        const onClick = jest.fn()
        const {dropdown} = setup({disabled: true, onClick})

        dropdown.instance().handleClick()

        expect(dropdown.state().isOpen).toBe(false)
        expect(onClick).not.toHaveBeenCalled()
      })
    })

    describe('handleSelection', () => {
      it('it calls onChoose with the item provided', () => {
        const onChoose = jest.fn(item => item)
        const {dropdown} = setup({onChoose})

        dropdown.simulate('click')
        dropdown.instance().handleSelection(itemOne)()
        expect(onChoose).toHaveBeenCalledTimes(1)
        expect(onChoose.mock.calls[0][0]).toEqual(itemOne)
      })

      it('closes the menu', () => {
        const {dropdown} = setup()
        dropdown.simulate('click')
        expect(dropdown.state().isOpen).toBe(true)

        dropdown.instance().handleSelection(itemOne)()
        expect(dropdown.state().isOpen).toBe(false)
      })

      it('resets state to defaults when opening', () => {
        const {
          dropdown,
          defaultState: {searchTerm, filteredItems, highlightedItemIndex},
        } = setup({items})

        dropdown.setState({
          searchTerm: 'foo',
          filteredItems: [itemOne],
          highlightedItemIndex: 100,
        })

        dropdown.instance().handleSelection(itemOne)()
        expect(dropdown.state().isOpen).toBe(true)
        expect(dropdown.state().searchTerm).toBe(searchTerm)
        expect(dropdown.state().filteredItems).toEqual(filteredItems)
        expect(dropdown.state().highlightedItemIndex).toBe(highlightedItemIndex)
      })
    })

    describe('handleFilterKeyPress', () => {
      describe('when Enter is pressed and there are items', () => {
        it('sets state of isOpen to false', () => {
          const {dropdown} = setup({items})
          dropdown.setState({isOpen: true})
          expect(dropdown.state().isOpen).toBe(true)

          dropdown.instance().handleFilterKeyPress({key: 'Enter'})
          expect(dropdown.state().isOpen).toBe(false)
        })

        it('fires onChoose with the items at the highlighted index', () => {
          const onChoose = jest.fn(item => item)
          const highlightedItemIndex = 1
          const {dropdown} = setup({items, onChoose})
          dropdown.setState({highlightedItemIndex})

          dropdown.instance().handleFilterKeyPress({key: 'Enter'})
          expect(onChoose).toHaveBeenCalledTimes(1)
          expect(onChoose.mock.calls[0][0]).toEqual(items[highlightedItemIndex])
        })
      })

      describe('when Escape is pressed', () => {
        it('sets isOpen state to false', () => {
          const {dropdown} = setup({items})
          dropdown.setState({isOpen: true})

          expect(dropdown.state().isOpen).toBe(true)

          dropdown.instance().handleFilterKeyPress({key: 'Escape'})

          expect(dropdown.state().isOpen).toBe(false)
        })
      })

      describe('when ArrowUp is pressed', () => {
        it('decrements the highlightedItemIndex', () => {
          const {dropdown} = setup({items})
          dropdown.setState({highlightedItemIndex: 1})

          dropdown.instance().handleFilterKeyPress({key: 'ArrowUp'})
          expect(dropdown.state().highlightedItemIndex).toBe(0)
        })

        it('does not decrement highlightedItemIndex past 0', () => {
          const {dropdown} = setup({items})
          dropdown.setState({highlightedItemIndex: 0})

          dropdown.instance().handleFilterKeyPress({key: 'ArrowUp'})
          expect(dropdown.state().highlightedItemIndex).toBe(0)
        })
      })

      describe('when ArrowDown is pressed', () => {
        describe('if no highlight has been set', () => {
          it('starts highlighted index at 0', () => {
            const {dropdown} = setup({items})
            expect(dropdown.state().highlightedItemIndex).toBe(null)

            dropdown.instance().handleFilterKeyPress({key: 'ArrowDown'})
            expect(dropdown.state().highlightedItemIndex).toBe(0)
          })
        })

        describe('if highlightedItemIndex has been set', () => {
          it('it increments the index', () => {
            const {dropdown} = setup({items})
            dropdown.setState({highlightedItemIndex: 0})

            dropdown.instance().handleFilterKeyPress({key: 'ArrowDown'})
            expect(dropdown.state().highlightedItemIndex).toBe(1)
          })

          describe('when highilghtedItemIndex is at the end of the list', () => {
            it('does not exceed the list length', () => {
              const {dropdown} = setup({items})
              dropdown.setState({highlightedItemIndex: 1})

              const expectedIndex = items.length - 1
              dropdown.instance().handleFilterKeyPress({key: 'ArrowDown'})
              expect(dropdown.state().highlightedItemIndex).toBe(expectedIndex)
            })
          })
        })
      })
    })
  })
})
