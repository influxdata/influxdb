import React, {Component} from 'react'
import PropTypes from 'prop-types'
import moment from 'moment'

import InputClickToEdit from 'shared/components/InputClickToEdit'

class GraphOptionsTimeFormat extends Component {
  state = {format: this.props.timeFormat || 'MM/DD/YYYY HH:mm:ss.ss'}

  handleInputChange = value => {
    const {onTimeFormatChange} = this.props
    const date = new Date()
    const formattedDate = moment(date.toISOString()).format(value)
    const validDateFormat = moment(formattedDate, value)._isValid
    if (validDateFormat) {
      onTimeFormatChange(value)
    }
  }

  render() {
    const {format} = this.state
    return (
      <div>
        <label>Time Format</label>
        <InputClickToEdit
          wrapperClass="fancytable--td orgs-table--name"
          value={format}
          onUpdate={this.handleInputChange}
          placeholder="MM/DD/YYYY HH:mm:ss.ss"
        />
      </div>
    )
  }
}

const {func, string} = PropTypes

GraphOptionsTimeFormat.propTypes = {
  timeFormat: string,
  onTimeFormatChange: func,
}

export default GraphOptionsTimeFormat
