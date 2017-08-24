import React, {Component, PropTypes} from 'react'
import Dropdown from 'src/shared/components/Dropdown'

import {
  QUERY_FILL_OPTIONS,
  NULL,
  NUMBER,
} from 'src/shared/constants/queryFillOptions'

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
    if (item.text === NUMBER) {
      this.setState({selected: item.text}, () => {
        this.props.onSelection(this.state.inputValue)
      })
    } else {
      this.setState({selected: item.text}, () => {
        this.props.onSelection(item.text)
      })
    }
  }

  handleInputBlur = e => {
    const inputValue = e.target.value || '0'

    this.setState({inputValue})
    this.props.onSelection(inputValue)
  }

  handleInputChange = e => {
    const inputValue = e.target.value

    this.setState({inputValue})
    this.props.onSelection(inputValue)
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
        <label>Fill</label>
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
