import React, {PropTypes, Component} from 'react'
import rome from 'rome'
import moment from 'moment'
import classNames from 'classnames'
import OnClickOutside from 'react-onclickoutside'

class CustomTimeRange extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isVisible: false,
    }
  }

  handleClickOutside() {
    this.setState({isVisible: false})
  }

  componentDidMount() {
    const lower = rome(this.timeLower, {
      initialValue: this._formatTimeRange(this.props.timeLower),
    })
    const upper = rome(this.timeUpper, {
      initialValue: this._formatTimeRange(this.props.timeUpper),
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
      <div className={classNames("custom-time-range", {show: this.state.isVisible})} style={{display: 'flex'}}>
        <div className="custom-time-container">
          <div className="time-lower" ref={(r) => this.timeLower = r} />
          <div className="time-upper" ref={(r) => this.timeUpper = r} />
          <div className="apply-time button" onClick={this.handleClick}>Apply</div>
        </div>
      </div>
    );
  }

  handleClick() {
    const lower = this.lowerCal.getDate().toISOString()
    const upper = this.upperCal.getDate().toISOString()

    this.props.onApplyTimeRange({
      lower: `'${lower}'`,
      upper: `'${upper}'`,
    })
  }
  /*
   * Upper and lower time ranges are passed in with single quotes as part of
   * the string literal, i.e. "'2015-09-23T18:00:00.000Z'".  Remove them
   * before passing the string to be parsed.
   */
  _formatTimeRange(timeRange) {
    if (!timeRange) {
      return '';
    }

    // If the given time range is relative, create a fixed timestamp based on its value
    if (timeRange.match(/^now/)) {
      const match = timeRange.match(/\d+\w/)[0];
      const duration = match.slice(0, match.length - 1);
      const unitOfTime = match[match.length - 1];
      return moment().subtract(duration, unitOfTime);
    }

    return moment(timeRange.replace(/\'/g, '')).format('YYYY-MM-DD HH:mm');
  }
}

const {
  func,
  string,
} = PropTypes

CustomTimeRange.propTypes = {
  onApplyTimeRange: func.isRequired,
  timeLower: string,
  timeUpper: string,
}

export default OnClickOutside(CustomTimeRange)
