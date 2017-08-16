import React, {PropTypes, Component} from 'react'
import rome from 'rome'
import moment from 'moment'

import shortcuts from 'hson!shared/data/shortcuts.hson'

class CustomTimeRange extends Component {
  constructor(props) {
    super(props)

    this.handleClick = ::this.handleClick
    this._formatTimeRange = ::this._formatTimeRange
    this.handleTimeRangeShortcut = ::this.handleTimeRangeShortcut
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
    return (
      <div className="custom-time--container">
        <div className="custom-time--shortcuts">
          <div className="custom-time--shortcuts-header">Shortcuts</div>
          {shortcuts.map(({id, name}) =>
            <div
              key={id}
              className="custom-time--shortcut"
              onClick={this.handleTimeRangeShortcut(id)}
            >
              {name}
            </div>
          )}
        </div>
        <div className="custom-time--wrap">
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
      const [, duration, unitOfTime] = timeRange.match(/(\d+)(\w+)/)
      moment().subtract(duration, unitOfTime)
    }

    return moment(timeRange.replace(/\'/g, '')).format('YYYY-MM-DD HH:mm')
  }

  handleClick() {
    const {onApplyTimeRange, onClose} = this.props
    const lower = this.lowerCal.getDate().toISOString()
    const upper = this.upperCal.getDate().toISOString()

    onApplyTimeRange({lower, upper})

    if (onClose) {
      onClose()
    }
  }

  handleTimeRangeShortcut(shortcut) {
    return () => {
      let lower
      const upper = moment()

      switch (shortcut) {
        case 'pastWeek': {
          lower = moment().subtract(1, 'week')
          break
        }
        case 'pastMonth': {
          lower = moment().subtract(1, 'month')
          break
        }
        case 'pastYear': {
          lower = moment().subtract(1, 'year')
          break
        }
        case 'thisWeek': {
          lower = moment().startOf('week')
          break
        }
        case 'thisMonth': {
          lower = moment().startOf('month')
          break
        }
        case 'thisYear': {
          lower = moment().startOf('year')
          break
        }
      }

      this.lowerCal.setValue(lower)
      this.upperCal.setValue(upper)
    }
  }
}

const {func, shape, string} = PropTypes

CustomTimeRange.propTypes = {
  onApplyTimeRange: func.isRequired,
  timeRange: shape({
    lower: string.isRequired,
    upper: string,
  }).isRequired,
  onClose: func,
}

export default CustomTimeRange
