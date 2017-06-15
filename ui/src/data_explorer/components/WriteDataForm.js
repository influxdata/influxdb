import React, {Component, PropTypes} from 'react'

import DatabaseDropdown from 'shared/components/DatabaseDropdown'
import OnClickOutside from 'shared/components/OnClickOutside'

import {OVERLAY_TECHNOLOGY} from 'shared/constants/classNames'

class WriteDataForm extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedDatabase: null,
      inputContent: '',
    }

    this.handleSelectDatabase = ::this.handleSelectDatabase
    this.handleSubmit = ::this.handleSubmit
    this.handleClickOutside = ::this.handleClickOutside
    this.handleKeyUp = ::this.handleKeyUp
    this.handleEdit = ::this.handleEdit
  }

  handleSelectDatabase(item) {
    this.setState({selectedDatabase: item.text})
  }

  handleClickOutside(e) {
    // guard against clicking to close error notification
    if (e.target.className === OVERLAY_TECHNOLOGY) {
      const {onClose} = this.props
      onClose()
    }
  }

  handleKeyUp(e) {
    e.stopPropagation()
    if (e.key === 'Escape') {
      const {onClose} = this.props
      onClose()
    }
  }

  async handleSubmit() {
    const {onClose, source, writeLineProtocol} = this.props
    const {inputContent, selectedDatabase} = this.state
    try {
      await writeLineProtocol(source, selectedDatabase, inputContent)
      onClose()
    } catch (error) {
      console.error(error.data.error)
    }
  }

  handleEdit(e) {
    this.setState({inputContent: e.target.value.trim()})
  }

  render() {
    const {onClose, errorThrown} = this.props
    const {inputContent, selectedDatabase} = this.state

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
            className="form-control write-data-form--input"
            autoComplete="off"
            spellCheck="false"
            placeholder="<measurement>,<tag_key>=<tag_value> <field_key>=<field_value>"
            onKeyUp={this.handleKeyUp}
            onChange={this.handleEdit}
            autoFocus={true}
          />
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
              onClick={this.handleSubmit}
              disabled={!inputContent}
            >
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
  writeLineProtocol: func.isRequired,
  errorThrown: func.isRequired,
}

export default OnClickOutside(WriteDataForm)
