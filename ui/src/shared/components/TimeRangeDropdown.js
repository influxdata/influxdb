import React from 'react'
import classnames from 'classnames'
import moment from 'moment'

import OnClickOutside from 'shared/components/OnClickOutside'
import FancyScrollbar from 'shared/components/FancyScrollbar'

import timeRanges from 'hson!shared/data/timeRanges.hson'
import {DROPDOWN_MENU_MAX_HEIGHT} from 'shared/constants/index'

const TimeRangeDropdown = React.createClass({
  autobind: false,

  propTypes: {
    selected: React.PropTypes.shape().isRequired,
    onChooseTimeRange: React.PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      isOpen: false,
    }
  },

  findTimeRangeInputValue({upper, lower}) {
    if (upper && lower) {
      const format = t =>
        moment(t.replace(/\'/g, '')).format('YYYY-MM-DD HH:mm')
      return `${format(lower)} - ${format(upper)}`
    }

    const selected = timeRanges.find(range => range.lower === lower)
    return selected ? selected.inputValue : 'Custom'
  },

  handleClickOutside() {
    this.setState({isOpen: false})
  },

  handleSelection(params) {
    const {lower, upper, menuOption} = params
    if (menuOption.toLowerCase() === 'custom') {
      this.props.onChooseTimeRange({custom: true})
    } else {
      this.props.onChooseTimeRange({lower, upper})
    }
    this.setState({isOpen: false})
  },

  toggleMenu() {
    this.setState({isOpen: !this.state.isOpen})
  },

  render() {
    const self = this
    const {selected} = self.props
    const {isOpen} = self.state

    return (
      <div className={classnames('dropdown dropdown-160', {open: isOpen})}>
        <div
          className="btn btn-sm btn-default dropdown-toggle"
          onClick={() => self.toggleMenu()}
        >
          <span className="icon clock" />
          <span className="dropdown-selected">
            {self.findTimeRangeInputValue(selected)}
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
            {timeRanges.map(item => {
              return (
                <li className="dropdown-item" key={item.menuOption}>
                  <a href="#" onClick={() => self.handleSelection(item)}>
                    {item.menuOption}
                  </a>
                </li>
              )
            })}
          </FancyScrollbar>
        </ul>
      </div>
    )
  },
})

export default OnClickOutside(TimeRangeDropdown)
