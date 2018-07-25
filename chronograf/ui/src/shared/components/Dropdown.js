import React, {Component} from 'react'
import PropTypes from 'prop-types'
import classnames from 'classnames'
import OnClickOutside from 'shared/components/OnClickOutside'
import DropdownMenu, {DropdownMenuEmpty} from 'shared/components/DropdownMenu'
import DropdownInput from 'shared/components/DropdownInput'
import DropdownHead from 'shared/components/DropdownHead'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
export class Dropdown extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
      searchTerm: '',
      filteredItems: this.props.items,
      highlightedItemIndex: null,
    }
  }

  static defaultProps = {
    actions: [],
    buttonSize: 'btn-sm',
    buttonColor: 'btn-default',
    menuWidth: '100%',
    useAutoComplete: false,
    disabled: false,
    tabIndex: 0,
  }

  handleClickOutside = () => {
    this.setState({isOpen: false})
  }

  handleClick = e => {
    const {disabled} = this.props

    if (disabled) {
      return
    }

    this.toggleMenu(e)
    if (this.props.onClick) {
      this.props.onClick(e)
    }
  }

  handleSelection = item => () => {
    this.toggleMenu()
    this.props.onChoose(item)
    this.dropdownRef.focus()
  }

  handleHighlight = itemIndex => () => {
    this.setState({highlightedItemIndex: itemIndex})
  }

  toggleMenu = e => {
    if (e) {
      e.stopPropagation()
    }

    if (!this.state.isOpen) {
      this.setState({
        searchTerm: '',
        filteredItems: this.props.items,
        highlightedItemIndex: null,
      })
    }

    this.setState({isOpen: !this.state.isOpen})
  }

  handleAction = (action, item) => e => {
    e.stopPropagation()
    action.handler(item)
  }

  handleFilterKeyPress = e => {
    const {filteredItems, highlightedItemIndex} = this.state

    if (e.key === 'Enter' && filteredItems.length) {
      this.setState({isOpen: false})
      this.props.onChoose(filteredItems[highlightedItemIndex])
    }
    if (e.key === 'Escape') {
      this.setState({isOpen: false})
    }
    if (e.key === 'ArrowUp' && highlightedItemIndex > 0) {
      this.setState({highlightedItemIndex: highlightedItemIndex - 1})
    }
    if (e.key === 'ArrowDown') {
      if (highlightedItemIndex < filteredItems.length - 1) {
        this.setState({highlightedItemIndex: highlightedItemIndex + 1})
      }
      if (highlightedItemIndex === null && filteredItems.length) {
        this.setState({highlightedItemIndex: 0})
      }
    }
  }

  handleFilterChange = e => {
    if (e.target.value) {
      return this.setState({searchTerm: e.target.value}, () =>
        this.applyFilter(this.state.searchTerm)
      )
    }

    this.setState({
      searchTerm: '',
      filteredItems: this.props.items,
      highlightedItemIndex: null,
    })
  }

  applyFilter = searchTerm => {
    const {items} = this.props
    const filterText = searchTerm.toLowerCase()
    const matchingItems = items.filter(item => {
      if (!item) {
        return false
      }

      return item.text.toLowerCase().includes(filterText)
    })

    this.setState({
      filteredItems: matchingItems,
      highlightedItemIndex: 0,
    })
  }

  render() {
    const {
      items,
      addNew,
      actions,
      selected,
      disabled,
      iconName,
      tabIndex,
      className,
      menuClass,
      menuWidth,
      menuLabel,
      buttonSize,
      buttonColor,
      toggleStyle,
      useAutoComplete,
    } = this.props

    const {isOpen, searchTerm, filteredItems, highlightedItemIndex} = this.state
    const menuItems = useAutoComplete ? filteredItems : items

    return (
      <div
        onClick={this.handleClick}
        className={classnames('dropdown', {
          open: isOpen,
          [className]: className,
        })}
        tabIndex={tabIndex}
        ref={r => (this.dropdownRef = r)}
        data-test="dropdown-toggle"
      >
        {useAutoComplete && isOpen ? (
          <DropdownInput
            searchTerm={searchTerm}
            buttonSize={buttonSize}
            buttonColor={buttonColor}
            toggleStyle={toggleStyle}
            disabled={disabled}
            onFilterChange={this.handleFilterChange}
            onFilterKeyPress={this.handleFilterKeyPress}
          />
        ) : (
          <DropdownHead
            iconName={iconName}
            selected={selected}
            searchTerm={searchTerm}
            buttonSize={buttonSize}
            buttonColor={buttonColor}
            toggleStyle={toggleStyle}
            disabled={disabled}
          />
        )}
        {isOpen && menuItems.length ? (
          <DropdownMenu
            addNew={addNew}
            actions={actions}
            items={menuItems}
            selected={selected}
            menuClass={menuClass}
            menuWidth={menuWidth}
            menuLabel={menuLabel}
            onAction={this.handleAction}
            useAutoComplete={useAutoComplete}
            onSelection={this.handleSelection}
            onHighlight={this.handleHighlight}
            highlightedItemIndex={highlightedItemIndex}
          />
        ) : (
          <DropdownMenuEmpty
            useAutoComplete={useAutoComplete}
            menuClass={menuClass}
          />
        )}
      </div>
    )
  }
}

const {arrayOf, bool, number, shape, string, func} = PropTypes

Dropdown.propTypes = {
  actions: arrayOf(
    shape({
      icon: string.isRequired,
      text: string.isRequired,
      handler: func.isRequired,
    })
  ),
  items: arrayOf(
    shape({
      text: string.isRequired,
    })
  ).isRequired,
  onChoose: func.isRequired,
  onClick: func,
  addNew: shape({
    url: string.isRequired,
    text: string.isRequired,
  }),
  selected: string.isRequired,
  iconName: string,
  className: string,
  buttonSize: string,
  buttonColor: string,
  menuWidth: string,
  menuLabel: string,
  menuClass: string,
  useAutoComplete: bool,
  toggleStyle: shape(),
  disabled: bool,
  tabIndex: number,
}

export default OnClickOutside(Dropdown)
