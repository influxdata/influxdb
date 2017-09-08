import React, {Component, PropTypes} from 'react'
import Dropdown from 'shared/components/Dropdown'

import {NULL_STRING, NUMBER} from 'shared/constants/queryFillOptions'

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

  handleDropdown = item => {
    if (item.text === NUMBER) {
      this.setState({selected: item}, () => {
        this.numberInput.focus()
      })
    } else {
      this.setState({selected: item}, () => {
        this.props.onChooseFill(item.text)
      })
    }
  }

  handleInputBlur = e => {
    if (!e.target.value) {
      this.setState({numberValue: '0'})
    }

    const numberValue = e.target.value || '0'

    this.setState({numberValue})
    this.props.onChooseFill(numberValue)
  }

  handleInputChange = e => {
    const numberValue = e.target.value

    this.setState({numberValue})
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

FillQuery.defaultProps = {
  size: 'sm',
  theme: 'blue',
  value: NULL_STRING,
}

FillQuery.propTypes = {
  onChooseFill: func.isRequired,
  value: string,
  size: string,
  theme: string,
}

export default FillQuery
