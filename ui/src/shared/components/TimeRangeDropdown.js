import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'
import moment from 'moment'

import OnClickOutside from 'shared/components/OnClickOutside'
import FancyScrollbar from 'shared/components/FancyScrollbar'

import timeRanges from 'hson!shared/data/timeRanges.hson'
import {DROPDOWN_MENU_MAX_HEIGHT} from 'shared/constants/index'

class TimeRangeDropdown extends Component {
  constructor(props) {
    super(props)
    this.state = {
      autobind: false,
      isOpen: false,
    }
    this.findTimeRangeInputValue = ::this.findTimeRangeInputValue
    this.handleSelection = ::this.handleSelection
    this.toggleMenu = ::this.toggleMenu
    this.showCustomTimeRange = ::this.showCustomTimeRange
  }

  findTimeRangeInputValue({upper, lower}) {
    if (upper && lower) {
      const format = t =>
        moment(t.replace(/\'/g, '')).format('YYYY-MM-DD HH:mm')
      return `${format(lower)} - ${format(upper)}`
    }

    const selected = timeRanges.find(range => range.lower === lower)
    return selected ? selected.inputValue : 'Custom'
  }

  handleClickOutside() {
    this.setState({isOpen: false})
  }

  handleSelection(params) {
    const {lower, upper, menuOption} = params
    if (menuOption.toLowerCase() === 'custom') {
      this.props.onChooseTimeRange({custom: true})
    } else {
      this.props.onChooseTimeRange({lower, upper})
    }
    this.setState({isOpen: false})
  }

  toggleMenu() {
    this.setState({isOpen: !this.state.isOpen})
  }

  showCustomTimeRange() {}

  render() {
    const {selected} = this.props
    const {isOpen} = this.state

    return (
      <div className={classnames('dropdown dropdown-160', {open: isOpen})}>
        <div
          className="btn btn-sm btn-default dropdown-toggle"
          onClick={() => this.toggleMenu()}
        >
          <span className="icon clock" />
          <span className="dropdown-selected">
            {this.findTimeRangeInputValue(selected)}
          </span>
          <span className="caret" />
        </div>
        <ul className="dropdown-menu">
          <FancyScrollbar
            autoHide={false}
            autoHeight={true}
            maxHeight={DROPDOWN_MENU_MAX_HEIGHT}
          >
            <li className="dropdown-header">Time Range</li>
            <li className="custom-timerange">
              <a href="#" onClick={this.showCustomTimeRange}>
                Custom Time Range
              </a>
            </li>
            {timeRanges.map(item => {
              return (
                <li className="dropdown-item" key={item.menuOption}>
                  <a href="#" onClick={() => this.handleSelection(item)}>
                    {item.menuOption}
                  </a>
                </li>
              )
            })}
          </FancyScrollbar>
        </ul>
      </div>
    )
  }
}

const {shape, func} = PropTypes

TimeRangeDropdown.propTypes = {
  selected: shape().isRequired,
  onChooseTimeRange: func.isRequired,
}

export default OnClickOutside(TimeRangeDropdown)
