import React, {PropTypes, Component} from 'react'
import OnClickOutside from 'react-onclickoutside'

import CustomTimeRange from 'shared/components/CustomTimeRange'

class CustomTimeRangeOverlay extends Component {
  constructor(props) {
    super(props)
  }

  handleClickOutside() {
    this.props.onClose()
  }

  render() {
    const {onClose, timeRange, onApplyTimeRange} = this.props

    return (
      <div className="custom-time--overlay">
        <CustomTimeRange
          onApplyTimeRange={onApplyTimeRange}
          timeRange={timeRange}
          onClose={onClose}
        />
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

CustomTimeRangeOverlay.propTypes = {
  onApplyTimeRange: func.isRequired,
  timeRange: shape({
    lower: string.isRequired,
    upper: string,
  }).isRequired,
  onClose: func,
}

export default OnClickOutside(CustomTimeRangeOverlay)
