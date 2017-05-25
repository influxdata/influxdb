import React, {Component, PropTypes} from 'react'

import DatabaseDropdown from 'shared/components/DatabaseDropdown'
import OnClickOutside from 'shared/components/OnClickOutside'

class WriteDataForm extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedDatabase: null,
    }

    this.handleSelectDatabase = ::this.handleSelectDatabase
    this.handleWrite = ::this.handleWrite
    this.handleClickOutside = ::this.handleClickOutside
  }

  handleSelectDatabase(item) {
    this.setState({selectedDatabase: item.text})
  }

  handleClickOutside() {
    const {onClose} = this.props
    onClose()
  }

  handleWrite() {
    const {onClose, source, writeData} = this.props
    const {selectedDatabase} = this.state
    writeData(source, selectedDatabase, this.editor.value).then(() => onClose())
  }

  render() {
    const {onClose, errorThrown} = this.props
    const {selectedDatabase} = this.state

    return (
      <div className="write-data-form">
        <div className="write-data-form--header">
          <div className="page-header__left">
            <h1 className="page-header__title">Write Data To</h1>
            <DatabaseDropdown
              onSelectDatabase={this.handleSelectDatabase}
              database={selectedDatabase}
              onErrorThrown={errorThrown}
            />
          </div>
          <div className="page-header__right">
            <span className="page-header__dismiss" onClick={onClose} />
          </div>
        </div>
        <div className="write-data-form--body">
          <textarea
            className="query-editor--field"
            autoComplete="off"
            spellCheck="false"
            placeholder="<measurement>,<tag_key>=<tag_value> <field_key>=<field_value>"
            ref={editor => this.editor = editor}
          />
          <span>
            Uses InfluxDB Line Protocol -&nbsp;
            <a
              href="https://docs.influxdata.com/influxdb/v1.2/write_protocols/line_protocol_tutorial/"
              target="_blank"
            >
              Docs
            </a>
          </span>
          <div className="page-header__right">
            <button className="btn btn-primary" onClick={this.handleWrite}>
              Write
            </button>
          </div>
        </div>
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

WriteDataForm.propTypes = {
  source: shape({
    links: shape({
      proxy: string.isRequired,
      self: string.isRequired,
      queries: string.isRequired,
    }).isRequired,
  }).isRequired,
  onClose: func.isRequired,
  writeData: func.isRequired,
  errorThrown: func.isRequired,
}

export default OnClickOutside(WriteDataForm)
