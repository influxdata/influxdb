import React, {PropTypes} from 'react'

const GraphOptionsTimeFormat = ({TimeFormat, onTimeFormatChange}) =>
  <div className="form-group col-xs-12">
    <label>Time Format</label>
    <input
      className="form-control input-sm"
      placeholder="mm/dd/yyyy HH:mm:ss.ss"
      value={TimeFormat}
      onChange={onTimeFormatChange}
    />
  </div>

const {func, string} = PropTypes

GraphOptionsTimeFormat.propTypes = {
  TimeFormat: string,
  onTimeFormatChange: func,
}

export default GraphOptionsTimeFormat
