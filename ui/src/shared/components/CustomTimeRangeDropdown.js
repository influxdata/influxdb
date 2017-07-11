import React, {PropTypes, Component} from 'react'
import moment from 'moment'
import classnames from 'classnames'
import OnClickOutside from 'react-onclickoutside'

import CustomTimeRange from 'shared/components/CustomTimeRange'

class CustomTimeRangeDropdown extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isDropdownOpen: false,
    }

    this.handleToggleDropdown = ::this.handleToggleDropdown
    this.handleCloseDropdown = ::this.handleCloseDropdown
  }

  handleClickOutside() {
    this.handleCloseDropdown()
  }

  handleToggleDropdown() {
    this.setState({isDropdownOpen: !this.state.isDropdownOpen})
  }

  handleCloseDropdown() {
    this.setState({isDropdownOpen: false})
  }

  render() {
    const {timeRange: {upper, lower}, timeRange, onApplyTimeRange} = this.props

    const {isDropdownOpen} = this.state

    return (
      <div
        className={classnames('custom-time-range', {open: isDropdownOpen})}
        style={{display: 'flex'}}
      >
        <button
          className="btn btn-sm btn-default dropdown-toggle"
          onClick={this.handleToggleDropdown}
        >
          <span className="icon clock" />
          <span className="dropdown-selected">{`${moment(lower).format(
            'MMM Do HH:mm'
          )} — ${moment(upper).format('MMM Do HH:mm')}`}</span>
          <span className="caret" />
        </button>
        <CustomTimeRange
          onApplyTimeRange={onApplyTimeRange}
          timeRange={timeRange}
          onClose={this.handleCloseDropdown}
        />
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

CustomTimeRangeDropdown.propTypes = {
  onApplyTimeRange: func.isRequired,
  timeRange: shape({
    lower: string.isRequired,
    upper: string.isRequired,
  }).isRequired,
}

export default OnClickOutside(CustomTimeRangeDropdown)
