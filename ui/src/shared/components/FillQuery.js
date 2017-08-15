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
  }

  handleInputBlur(e) {
    const {useCustomNumber, inputValue} = this.state
    // Prevent user from submitting an empty string as their value
    // Use 0 by default
    if (e.target.value === '') {
      this.setState({inputValue: '0'})
    }
    if (useCustomNumber) {
      this.props.onSelection(inputValue)
    }
  }

  handleInputChange(e) {
    this.setState({inputValue: e.target.value})
  }

  handleDropdown(item) {
    if (item.text === 'number') {
      this.setState({selected: item.text, useCustomNumber: true}, () => {
        this.props.onSelection(this.state.inputValue)
      })
    } else {
      this.setState({selected: item.text, useCustomNumber: false}, () => {
        this.props.onSelection(item.text)
      })
    }
  }

  handleKeyUp(e) {
    if (e.key === 'Enter' || e.key === 'Escape') {
      e.target.blur()
    }
  }

  render() {
    const {size} = this.props
    const {selected, useCustomNumber, inputValue} = this.state
    const items = QUERY_FILL_OPTIONS.map(text => ({text}))

    return (
      <div className={`fill-query fill-query--${size}`}>
        <label>Fill missing with</label>
        <Dropdown
          selected={selected}
          items={items}
          className="fill-query--dropdown"
          buttonSize={`btn-${size}`}
          buttonColor="btn-default"
          onChoose={this.handleDropdown}
        />
        {useCustomNumber
          ? <input
              type="number"
              className={`form-control monotype form-plutonium input-${size} fill-query--input`}
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
}

export default FillQuery
