import React, {Component} from 'react'
import PropTypes from 'prop-types'

import InputClickToEdit from 'shared/components/InputClickToEdit'
import {Dropdown} from 'src/shared/components/Dropdown'
import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'

const formatOptions = [
  {text: 'MM/DD/YYYY HH:mm:ss.ss'},
  {text: 'MM/DD/YYYY HH:mm'},
  {text: 'MM/DD/YYYY'},
  {text: 'h:mm:ss A'},
  {text: 'h:mm A'},
  {text: 'MMMM D, YYYY'},
  {text: 'MMMM D, YYYY h:mm A'},
  {text: 'dddd, MMMM D, YYYY h:mm A'},
  {text: 'Custom'},
]

class GraphOptionsTimeFormat extends Component {
  state = {
    format: this.props.timeFormat || 'MM/DD/YYYY HH:mm:ss.ss',
    customFormat: false,
  }

  handleChooseFormat = formatOption => {
    const {onTimeFormatChange} = this.props
    if (formatOption.text === 'Custom') {
      this.setState({customFormat: true})
    } else {
      this.setState({format: formatOption.text, customFormat: false})
      onTimeFormatChange(formatOption.text)
    }
  }

  render() {
    const {format, customFormat} = this.state
    const {onTimeFormatChange} = this.props
    const tipContent =
      'For information on formatting, see http://momentjs.com/docs/#/parsing/string-format/'

    return (
      <div className="form-group col-xs-12">
        <label>
          Time Format
          {customFormat &&
            <QuestionMarkTooltip
              tipID="Time Axis Format"
              tipContent={tipContent}
            />}
        </label>
        <Dropdown
          items={formatOptions}
          selected={customFormat ? 'Custom' : format}
          buttonColor="btn-default"
          buttonSize="btn-xs"
          className="dropdown-stretch"
          onChoose={this.handleChooseFormat}
        />
        {customFormat &&
          <div className="column-controls--section">
            <InputClickToEdit
              wrapperClass="column-controls-input "
              value={format}
              onUpdate={onTimeFormatChange}
              placeholder="Enter custom format..."
              appearAsNormalInput={true}
            />
          </div>}
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
