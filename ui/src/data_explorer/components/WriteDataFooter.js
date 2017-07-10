import React, {PropTypes} from 'react'

const WriteDataFooter = ({
  isManual,
  inputContent,
  uploadContent,
  handleSubmit,
}) => (
  <div className="write-data-form--footer">
    {isManual
      ? <span className="write-data-form--helper">
          Need help writing InfluxDB Line Protocol? -&nbsp;
          <a
            href="https://docs.influxdata.com/influxdb/latest/write_protocols/line_protocol_tutorial/"
            target="_blank"
          >
            See Documentation
          </a>
        </span>
      : <span className="write-data-form--helper">
          <a
            href="https://docs.influxdata.com/influxdb/v1.2//tools/shell/#import-data-from-a-file-with-import"
            target="_blank"
          >
            File Upload Documentation
          </a>
        </span>}
    <button
      className="btn btn-sm btn-success write-data-form--submit"
      onClick={handleSubmit}
      disabled={(!inputContent && isManual) || (!uploadContent && !isManual)}
    >
      Write
    </button>
  </div>
)

const {bool, func, string} = PropTypes

WriteDataFooter.propTypes = {
  isManual: bool.isRequired,
  uploadContent: string,
  inputContent: string,
  handleSubmit: func,
}

export default WriteDataFooter
