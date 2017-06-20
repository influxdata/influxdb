import React, {PropTypes} from 'react'

const WriteData = ({
  handleKeyUp,
  handleFile,
  handleEdit,
  handleSubmit,
  inputContent,
  isManual,
}) => (
  <div className="write-data-form--body">
    {isManual
      ? <textarea
          className="form-control write-data-form--input"
          autoComplete="off"
          spellCheck="false"
          placeholder="<measurement>,<tag_key>=<tag_value> <field_key>=<field_value>"
          onKeyUp={handleKeyUp}
          onChange={handleEdit}
          autoFocus={true}
        />
      : <input type="file" onChange={handleFile} />}
    <div className="write-data-form--footer">
      <span className="write-data-form--helper">
        Need help writing InfluxDB Line Protocol? -&nbsp;
        <a
          href="https://docs.influxdata.com/influxdb/latest/write_protocols/line_protocol_tutorial/"
          target="_blank"
        >
          See Documentation
        </a>
      </span>
      <button
        className="btn btn-sm btn-primary write-data-form--submit"
        onClick={handleSubmit}
        disabled={!inputContent}
      >
        Write
      </button>
    </div>
  </div>
)

const {func, string, bool} = PropTypes

WriteData.propTypes = {
  handleKeyUp: func.isRequired,
  handleEdit: func.isRequired,
  handleFile: func.isRequired,
  handleSubmit: func.isRequired,
  inputContent: string,
  isManual: bool,
}

export default WriteData
