import React, {Component, PropTypes} from 'react'
import Dropdown from 'src/shared/components/Dropdown'

import {QUERY_FILL_OPTIONS} from 'src/shared/constants/queryFillOptions'

class FillQuery extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selected: QUERY_FILL_OPTIONS[0],
      useCustomNumber: false,
      customNumber: null,
      inputValue: '0',
    }

    this.handleDropdown = ::this.handleDropdown
    this.handleInputBlur = ::this.handleInputBlur
    this.handleInputChange = ::this.handleInputChange
    this.handleKeyUp = ::this.handleKeyUp
  }

  static defaultProps = {
    size: 'xs',
    theme: 'blue',
  }

  handleInputBlur(e) {
    const {useCustomNumber, inputValue} = this.state
    // Prevent user from submitting an empty string as their value
    // Use 0 by default
    if (e.target.value === '') {
      const zeroVal = '0'
      this.setState({inputValue: zeroVal}, () => {
        this.props.onSelection(zeroVal, true)
      })
    } else if (useCustomNumber) {
      this.props.onSelection(inputValue, true)
    }
  }

  handleInputChange(e) {
    this.setState({inputValue: e.target.value})
  }

  handleDropdown(item) {
    if (item.text === 'number') {
      this.setState({selected: item.text, useCustomNumber: true}, () => {
        this.props.onSelection(this.state.inputValue, true)
      })
    } else {
      this.setState({selected: item.text, useCustomNumber: false}, () => {
        this.props.onSelection(item.text, false)
      })
    }
  }

  handleKeyUp(e) {
    if (e.key === 'Enter' || e.key === 'Escape') {
      e.target.blur()
    }
  }

  render() {
    const {size, theme} = this.props
    const {selected, useCustomNumber, inputValue} = this.state
    const items = QUERY_FILL_OPTIONS.map(text => ({text}))

    let inputTheme = ''
    let dropdownTheme = ''

    if (theme === 'blue') {
      inputTheme = 'form-plutonium'
    }
    if (theme === 'green') {
      inputTheme = 'form-malachite'
      dropdownTheme = 'dropdown-malachite'
    }
    if (theme === 'purple') {
      inputTheme = 'form-astronaut'
      dropdownTheme = 'dropdown-astronaut'
    }

    return (
      <div className={`fill-query fill-query--${size}`}>
        <label>Fill missing with</label>
        <Dropdown
          selected={selected}
          items={items}
          className="fill-query--dropdown"
          buttonSize={`btn-${size}`}
          buttonColor="btn-default"
          menuClass={dropdownTheme}
          onChoose={this.handleDropdown}
        />
        {useCustomNumber
          ? <input
              type="number"
              className={`form-control monotype ${inputTheme} input-${size} fill-query--input`}
              placeholder="Custom Value"
              autoFocus={true}
              value={inputValue}
              onKeyUp={this.handleKeyUp}
              onChange={this.handleInputChange}
              onBlur={this.handleInputBlur}
            />
          : null}
      </div>
    )
  }
}

const {func, string} = PropTypes

FillQuery.propTypes = {
  onSelection: func.isRequired,
  size: string,
  theme: string,
}

export default FillQuery
