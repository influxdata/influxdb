import React, {Component, PropTypes} from 'react'

import OnClickOutside from 'shared/components/OnClickOutside'
import WriteDataBody from 'src/data_explorer/components/WriteDataBody'
import WriteDataHeader from 'src/data_explorer/components/WriteDataHeader'

import {OVERLAY_TECHNOLOGY} from 'shared/constants/classNames'

class WriteDataForm extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedDatabase: null,
      inputContent: null,
      uploadContent: '',
      progress: '',
      isManual: false,
    }

    this.handleSelectDatabase = ::this.handleSelectDatabase
    this.handleSubmit = ::this.handleSubmit
    this.handleClickOutside = ::this.handleClickOutside
    this.handleKeyUp = ::this.handleKeyUp
    this.handleEdit = ::this.handleEdit
    this.handleFile = ::this.handleFile
    this.toggleWriteView = ::this.toggleWriteView
  }

  toggleWriteView(isManual) {
    this.setState({isManual})
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

  handleFile(e) {
    // todo: expect this to be a File or Blob
    const file = e.target.files[0]
    const reader = new FileReader()
    reader.readAsText(file)

    // async function run when loading of file is complete
    reader.onload = loadEvent => {
      this.setState({inputContent: loadEvent.target.result})
    }
  }

  render() {
    const {onClose, errorThrown} = this.props
    const {inputContent, selectedDatabase, isManual} = this.state

    return (
      <div className="write-data-form">
        <WriteDataHeader
          handleSelectDatabase={this.handleSelectDatabase}
          selectedDatabase={selectedDatabase}
          errorThrown={errorThrown}
          toggleWriteView={this.toggleWriteView}
          isManual={isManual}
          onClose={onClose}
        />
        <WriteDataBody
          isManual={isManual}
          inputContent={inputContent}
          handleEdit={this.handleEdit}
          handleFile={this.handleFile}
          handleKeyUp={this.handleKeyUp}
          handleSubmit={this.handleSubmit}
        />
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
