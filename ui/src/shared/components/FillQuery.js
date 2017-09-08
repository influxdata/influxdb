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
          currentNumberValue: props.value,
          resetNumberValue: props.value,
        }
      : {
          selected: queryFills.find(fill => fill.type === props.value),
          currentNumberValue: '0',
          resetNumberValue: '0',
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
    const nextNumberValue = e.target.value
      ? e.target.value
      : this.state.resetNumberValue || '0'

    this.setState({
      currentNumberValue: nextNumberValue,
      resetNumberValue: nextNumberValue,
    })

    this.props.onChooseFill(nextNumberValue)
  }

  handleInputChange = e => {
    const currentNumberValue = e.target.value

    this.setState({currentNumberValue})
  }

  handleKeyDown = e => {
    if (e.key === 'Enter') {
      this.numberInput.blur()
    }
  }

  handleKeyUp = e => {
    if (e.key === 'Escape') {
      this.setState({currentNumberValue: this.state.resetNumberValue}, () => {
        this.numberInput.blur()
      })
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
    const {selected, currentNumberValue} = this.state

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
            value={currentNumberValue}
            onKeyUp={this.handleKeyUp}
            onKeyDown={this.handleKeyDown}
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
