import React, {PropTypes, Component} from 'react'
import rome from 'rome'
import moment from 'moment'
import classnames from 'classnames'
import OnClickOutside from 'react-onclickoutside'

class CustomTimeRangeDropdown extends Component {
  constructor(props) {
    super(props)

    this.handleClick = ::this.handleClick
  }

  handleClickOutside() {
    this.props.onClose()
  }

  componentDidMount() {
    const {timeRange} = this.props

    const lower = rome(this.lower, {
      initialValue: this._formatTimeRange(timeRange.lower),
    })
    const upper = rome(this.upper, {
      initialValue: this._formatTimeRange(timeRange.upper),
    })

    this.lowerCal = lower
    this.upperCal = upper
  }

  // If there is an upper or lower time range set, set the corresponding calendar's value.
  componentWillReceiveProps(nextProps) {
    const {lower, upper} = nextProps.timeRange
    if (lower) {
      this.lowerCal.setValue(this._formatTimeRange(lower))
    }

    if (upper) {
      this.upperCal.setValue(this._formatTimeRange(upper))
    }
  }

  render() {
    const {isVisible, onToggle, timeRange: {upper, lower}} = this.props

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
          <div className="custom-time--dates">
            <div className="custom-time--lower" ref={r => (this.lower = r)} />
            <div className="custom-time--upper" ref={r => (this.upper = r)} />
          </div>
          <div
            className="custom-time--apply btn btn-sm btn-primary"
            onClick={this.handleClick}
          >
            Apply
          </div>
        </div>
      </div>
    )
  }

  handleClick() {
    const lower = this.lowerCal.getDate().toISOString()
    const upper = this.upperCal.getDate().toISOString()

    this.props.onApplyTimeRange({lower, upper})
    this.props.onClose()
  }
  /*
   * Upper and lower time ranges are passed in with single quotes as part of
   * the string literal, i.e. "'2015-09-23T18:00:00.000Z'".  Remove them
   * before passing the string to be parsed.
   */
  _formatTimeRange(timeRange) {
    if (!timeRange) {
      return ''
    }

    // If the given time range is relative, create a fixed timestamp based on its value
    if (timeRange.match(/^now/)) {
      const match = timeRange.match(/\d+\w/)[0]
      const duration = match.slice(0, match.length - 1)
      const unitOfTime = match[match.length - 1]
      return moment().subtract(duration, unitOfTime)
    }

    return moment(timeRange.replace(/\'/g, '')).format('YYYY-MM-DD HH:mm')
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
