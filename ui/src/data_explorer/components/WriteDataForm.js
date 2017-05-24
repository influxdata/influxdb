import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import DatabaseDropdown from 'shared/components/DatabaseDropdown'

import {writeData} from 'src/data_explorer/apis'
import {publishAutoDismissingNotification} from 'shared/dispatchers'
import {errorThrown as errorThrownAction} from 'shared/actions/errors'

class WriteDataForm extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedDatabase: null,
    }

    this.handleSelectDatabase = ::this.handleSelectDatabase
    this.handleError = ::this.handleError
    this.handleWrite = ::this.handleWrite
  }

  handleSelectDatabase(item) {
    this.setState({selectedDatabase: item.text})
  }

  handleError(error) {
    const {errorThrown} = this.props
    errorThrown(error)
  }

  async handleWrite() {
    const {onClose, source, notify, errorThrown} = this.props
    const {selectedDatabase} = this.state
    try {
      await writeData(source, selectedDatabase, this.editor.value)
      notify('success', 'Data was written successfully')
      onClose()
    } catch (response) {
      errorThrown(response, response.data.error)
    }
  }

  render() {
    const {onClose} = this.props
    const {selectedDatabase} = this.state

    return (
      <div className="write-data-form">
        <div className="write-data-form--header">
          <div className="page-header__left">
            <h1 className="page-header__title">Write Data To</h1>
            <DatabaseDropdown
              onSelectDatabase={this.handleSelectDatabase}
              database={selectedDatabase}
              onStartEdit={() => {}}
              onErrorThrown={this.handleError}
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
  notify: func.isRequired,
  errorThrown: func.isRequired,
  onClose: func.isRequired,
}

const mapDispatchToProps = dispatch => ({
  notify: bindActionCreators(publishAutoDismissingNotification, dispatch),
  errorThrown: bindActionCreators(errorThrownAction, dispatch),
})

export default connect(null, mapDispatchToProps)(WriteDataForm)
