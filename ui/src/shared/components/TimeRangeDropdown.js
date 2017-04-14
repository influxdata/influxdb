import React from 'react'
import classnames from 'classnames'
import OnClickOutside from 'shared/components/OnClickOutside'

import moment from 'moment'

import timeRanges from 'hson!../data/timeRanges.hson'

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
      const format = (t) => moment(t.replace(/\'/g, '')).format('YYYY-MM-DD HH:mm')
      return `${format(lower)} - ${format(upper)}`
    }

    const selected = timeRanges.find((range) => range.lower === lower)
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
      <div className="dropdown dropdown-160">
        <div className="btn btn-sm btn-info dropdown-toggle" onClick={() => self.toggleMenu()}>
          <span className="icon clock"></span>
          <span className="selected-time-range">{self.findTimeRangeInputValue(selected)}</span>
          <span className="caret" />
        </div>
        <ul className={classnames("dropdown-menu", {show: isOpen})}>
          <li className="dropdown-header">Time Range</li>
          {timeRanges.map((item) => {
            return (
              <li key={item.menuOption}>
                <a href="#" onClick={() => self.handleSelection(item)}>
                  {item.menuOption}
                </a>
              </li>
            )
          })}
        </ul>
      </div>
    )
  },
})

export default OnClickOutside(TimeRangeDropdown)
