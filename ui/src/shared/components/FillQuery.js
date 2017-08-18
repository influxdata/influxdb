import React, {Component, PropTypes} from 'react'
import Dropdown from 'src/shared/components/Dropdown'

import {
  QUERY_FILL_OPTIONS,
  NUMBER,
  NULL,
} from 'src/shared/constants/queryFillOptions'

/*
  NOTE
  This component requires a function be passed in as a prop
  FillQuery calls that function with 2 arguments:
   - Either the selected item from the dropdown or the value of the input
   - A boolean to type the result, true = number, false = string
*/
class FillQuery extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selected: this.props.fillType,
      inputValue: '0',
    }
  }

  static defaultProps = {
    size: 'xs',
    theme: 'blue',
    fillType: NULL,
  }

  handleDropdown = item => {
    if (item.text === 'number') {
      this.setState({selected: item.text}, () => {
        this.props.onSelection(this.state.inputValue, true)
      })
    } else {
      this.setState({selected: item.text}, () => {
        this.props.onSelection(item.text, false)
      })
    }
  }

  handleInputBlur = e => {
    const inputValue = e.target.value || '0'

    this.setState({inputValue})
    this.props.onSelection(inputValue, true)
  }

  handleInputChange = e => {
    this.setState({inputValue: e.target.value})
  }

  handleKeyUp = e => {
    if (e.key === 'Enter' || e.key === 'Escape') {
      e.target.blur()
    }
  }

  getColor = theme => {
    switch (theme) {
      case 'BLUE':
        return 'plutonium'
      case 'GREEN':
        return 'malachite'
      case 'PURPLE':
        return 'astronaut'
      default:
        return 'plutonium'
    }
  }

  render() {
    const {size, theme} = this.props
    const {selected, inputValue} = this.state
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
          menuClass={`dropdown-${this.getColor(theme)}`}
          onChoose={this.handleDropdown}
        />
        {selected === NUMBER &&
          <input
            type="number"
            className={`form-control monotype form-${this.getColor(
              theme
            )} input-${size} fill-query--input`}
            placeholder="Custom Value"
            autoFocus={true}
            value={inputValue}
            onKeyUp={this.handleKeyUp}
            onChange={this.handleInputChange}
            onBlur={this.handleInputBlur}
          />}
      </div>
    )
  }
}

const {func, string} = PropTypes

FillQuery.propTypes = {
  onSelection: func.isRequired,
  fillType: string,
  size: string,
  theme: string,
}

export default FillQuery
