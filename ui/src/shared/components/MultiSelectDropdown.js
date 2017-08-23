import React, {Component, PropTypes} from 'react'

import classnames from 'classnames'
import _ from 'lodash'

import OnClickOutside from 'shared/components/OnClickOutside'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import {DROPDOWN_MENU_MAX_HEIGHT} from 'shared/constants/index'

const labelText = ({localSelectedItems, isOpen, label}) => {
  if (label) {
    return label
  } else if (localSelectedItems.length) {
    return localSelectedItems.map(s => s).join(', ')
  }

  // TODO: be smarter about the text displayed here
  if (isOpen) {
    return '0 Selected'
  }
  return 'None'
}

class MultiSelectDropdown extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
      localSelectedItems: this.props.selectedItems,
    }

    this.onSelect = ::this.onSelect
    this.onApplyFunctions = ::this.onApplyFunctions
  }

  handleClickOutside() {
    this.setState({isOpen: false})
  }

  toggleMenu = e => {
    e.stopPropagation()
    this.setState({isOpen: !this.state.isOpen})
  }

  onSelect(item, e) {
    e.stopPropagation()

    const {localSelectedItems} = this.state

    let nextItems
    if (this.isSelected(item)) {
      nextItems = localSelectedItems.filter(i => i !== item)
    } else {
      nextItems = [...localSelectedItems, item]
    }

    this.setState({localSelectedItems: nextItems})
  }

  isSelected(item) {
    return !!this.state.localSelectedItems.find(text => text === item)
  }

  onApplyFunctions(e) {
    e.stopPropagation()

    this.setState({isOpen: false})
    this.props.onApply(this.state.localSelectedItems)
  }

  render() {
    const {localSelectedItems, isOpen} = this.state
    const {label, buttonSize, buttonColor, customClass, iconName} = this.props

    return (
      <div className={classnames(`dropdown ${customClass}`, {open: isOpen})}>
        <div
          onClick={this.toggleMenu}
          className={`dropdown-toggle btn ${buttonSize} ${buttonColor}`}
        >
          {iconName ? <span className={`icon ${iconName}`} /> : null}
          <span className="dropdown-selected">
            {labelText({localSelectedItems, isOpen, label})}
          </span>
          <span className="caret" />
        </div>
        {this.renderMenu()}
      </div>
    )
  }

  renderMenu() {
    const {items} = this.props

    return (
      <ul className="dropdown-menu">
        <li className="multi-select--apply">
          <button
            className="btn btn-xs btn-info"
            onClick={this.onApplyFunctions}
          >
            Apply
          </button>
        </li>
        <FancyScrollbar
          autoHide={false}
          autoHeight={true}
          maxHeight={DROPDOWN_MENU_MAX_HEIGHT}
        >
          {items.map((listItem, i) => {
            return (
              <li
                key={i}
                className={classnames('multi-select--item', {
                  checked: this.isSelected(listItem),
                })}
                onClick={_.wrap(listItem, this.onSelect)}
              >
                <div className="multi-select--checkbox" />
                <span>
                  {listItem}
                </span>
              </li>
            )
          })}
        </FancyScrollbar>
      </ul>
    )
  }
}

const {arrayOf, func, string} = PropTypes

MultiSelectDropdown.propTypes = {
  onApply: func.isRequired,
  items: arrayOf(string.isRequired).isRequired,
  selectedItems: arrayOf(string.isRequired).isRequired,
  label: string,
  buttonSize: string,
  buttonColor: string,
  customClass: string,
  iconName: string,
}
MultiSelectDropdown.defaultProps = {
  buttonSize: 'btn-sm',
  buttonColor: 'btn-default',
  customClass: 'dropdown-160',
}

export default OnClickOutside(MultiSelectDropdown)
