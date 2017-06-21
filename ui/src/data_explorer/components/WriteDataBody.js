import React, {PropTypes} from 'react'

const WriteDataBody = ({
  handleKeyUp,
  handleFile,
  handleEdit,
  handleSubmit,
  inputContent,
  uploadContent,
  isManual,
}) => {
  let inputRef

  return (
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
        : <div className="write-data-form--file">
            <input
              type="file"
              onChange={handleFile}
              className="write-data-form--upload"
              ref={r => (inputRef = r)}
            />
            <button
              className="btn btn-sm btn-primary"
              onClick={() => inputRef.click()}
            >
              {uploadContent ? 'Upload a Different File' : 'Upload a File'}
            </button>
            {uploadContent
              ? <span className="write-data-form--filepath_selected">
                  <span className="icon checkmark" />1 file selected
                </span>
              : <span className="write-data-form--filepath_empty">
                  No file selected
                </span>}
          </div>}
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
          disabled={
            (!inputContent && isManual) || (!uploadContent && !isManual)
          }
        >
          Write
        </button>
      </div>
    </div>
  )
}

const {func, string, bool} = PropTypes

WriteDataBody.propTypes = {
  handleKeyUp: func.isRequired,
  handleEdit: func.isRequired,
  handleFile: func.isRequired,
  handleSubmit: func.isRequired,
  inputContent: string,
  uploadContent: string,
  isManual: bool,
}

export default WriteDataBody
