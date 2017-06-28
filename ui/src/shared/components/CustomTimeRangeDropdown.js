import React, {PropTypes, Component} from 'react'
import moment from 'moment'
import classnames from 'classnames'
import OnClickOutside from 'react-onclickoutside'

import CustomTimeRange from 'shared/components/CustomTimeRange'

class CustomTimeRangeDropdown extends Component {
  constructor(props) {
    super(props)
  }

  handleClickOutside() {
    this.props.onClose()
  }

  render() {
    const {
      isVisible,
      onToggle,
      onClose,
      timeRange: {upper, lower},
      timeRange,
      onApplyTimeRange,
    } = this.props

    return (
      <div
        className={classnames('custom-time-range', {open: isVisible})}
        style={{display: 'flex'}}
      >
        <button
          className="btn btn-sm btn-default dropdown-toggle"
          onClick={onToggle}
        >
          <span className="icon clock" />
          <span className="dropdown-selected">{`${moment(lower).format(
            'MMM Do HH:mm'
          )} — ${moment(upper).format('MMM Do HH:mm')}`}</span>
          <span className="caret" />
        </button>
        <div className="custom-time--container">
          <CustomTimeRange
            onApplyTimeRange={onApplyTimeRange}
            timeRange={timeRange}
            onClose={onClose}
          />
        </div>
      </div>
    )
  }
}

const {bool, func, shape, string} = PropTypes

CustomTimeRangeDropdown.propTypes = {
  onApplyTimeRange: func.isRequired,
  timeRange: shape({
    lower: string.isRequired,
    upper: string.isRequired,
  }).isRequired,
  isVisible: bool.isRequired,
  onToggle: func.isRequired,
  onClose: func.isRequired,
}

export default OnClickOutside(CustomTimeRangeDropdown)
