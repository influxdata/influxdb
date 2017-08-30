import React, {Component, PropTypes} from 'react'
import Dropdown from 'shared/components/Dropdown'

import {NULL, NUMBER} from 'shared/constants/queryFillOptions'

import queryFills from 'hson!shared/data/queryFills.hson'

class FillQuery extends Component {
  constructor(props) {
    super(props)

    const isNumberValue = !isNaN(Number(props.value))

    this.state = isNumberValue
      ? {
          selected: queryFills.find(fill => fill.type === NUMBER),
          numberValue: props.value,
        }
      : {
          selected: queryFills.find(fill => fill.type === props.value),
          numberValue: '0',
        }
  }

  static defaultProps = {
    size: 'sm',
    theme: 'blue',
    value: NULL,
  }

  handleDropdown = item => {
    if (item.text === NUMBER) {
      this.setState({selected: item}, () => {
        this.props.onSelection(this.state.numberValue)
        this.numberInput.focus()
      })
    } else {
      this.setState({selected: item}, () => {
        this.props.onSelection(item.text)
      })
    }
  }

  handleInputBlur = e => {
    const numberValue = e.target.value || '0'

    this.setState({numberValue})
    this.props.onSelection(numberValue)
  }

  handleInputChange = e => {
    const numberValue = e.target.value

    this.setState({numberValue})
    this.props.onSelection(numberValue)
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
    const {selected, numberValue} = this.state

    return (
      <div className={`fill-query fill-query--${size}`}>
        {selected.type === NUMBER &&
          <input
            ref={r => (this.numberInput = r)}
            type="number"
            className={`form-control monotype form-${this.getColor(
              theme
            )} input-${size} fill-query--input`}
            placeholder="Custom Value"
            value={numberValue}
            onKeyUp={this.handleKeyUp}
            onChange={this.handleInputChange}
            onBlur={this.handleInputBlur}
          />}
        <Dropdown
          selected={selected.text}
          items={queryFills}
          className="fill-query--dropdown dropdown-100"
          buttonSize={`btn-${size}`}
          buttonColor="btn-info"
          menuClass={`dropdown-${this.getColor(theme)}`}
          onChoose={this.handleDropdown}
        />
        <label className="fill-query--label">Fill:</label>
      </div>
    )
  }
}

const {func, string} = PropTypes

FillQuery.propTypes = {
  onSelection: func.isRequired,
  value: string,
  size: string,
  theme: string,
}

export default FillQuery
