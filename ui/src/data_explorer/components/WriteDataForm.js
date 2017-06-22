import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'

import OnClickOutside from 'shared/components/OnClickOutside'
import WriteDataBody from 'src/data_explorer/components/WriteDataBody'
import WriteDataHeader from 'src/data_explorer/components/WriteDataHeader'

import {OVERLAY_TECHNOLOGY} from 'shared/constants/classNames'

class WriteDataForm extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedDatabase: props.selectedDatabase,
      inputContent: null,
      uploadContent: '',
      fileName: '',
      progress: '',
      isManual: false,
      dragClass: 'drag-none',
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
    const {inputContent, uploadContent, selectedDatabase, isManual} = this.state
    const content = isManual ? inputContent : uploadContent

    try {
      await writeLineProtocol(source, selectedDatabase, content)
      onClose()
      window.location.reload()
    } catch (error) {
      console.error(error.data.error)
    }
  }

  handleEdit(e) {
    this.setState({inputContent: e.target.value.trim()})
  }

  handleFile(e, drop) {
    // todo: expect this to be a File or Blob
    let file
    if (drop) {
      file = e.dataTransfer.files[0]
    } else {
      file = e.target.files[0]
    }

    e.preventDefault()
    e.stopPropagation()

    // async function run when loading of file is complete
    const reader = new FileReader()
    reader.readAsText(file)
    reader.onload = loadEvent => {
      this.setState({
        uploadContent: loadEvent.target.result,
        fileName: file.name,
      })
    }
  }

  handleDragOver(e) {
    e.preventDefault()
    e.stopPropagation()
  }

  handleDragClass(entering) {
    return e => {
      e.preventDefault()
      if (entering) {
        this.setState({
          dragClass: 'drag-over',
        })
      } else {
        this.setState({
          dragClass: 'drag-none',
        })
      }
    }
  }

  render() {
    const {onClose, errorThrown} = this.props
    const {dragClass} = this.state

    return (
      <div
        onDrop={e => this.handleFile(e, true)}
        onDragOver={this.handleDragOver}
        onDragEnter={this.handleDragClass(true)}
        onDragLeave={this.handleDragClass(false)}
        className={classnames(OVERLAY_TECHNOLOGY, dragClass)}
      >
        <div className="write-data-form">
          <WriteDataHeader
            {...this.state}
            handleSelectDatabase={this.handleSelectDatabase}
            errorThrown={errorThrown}
            toggleWriteView={this.toggleWriteView}
            onClose={onClose}
          />
          <WriteDataBody
            {...this.state}
            handleEdit={this.handleEdit}
            handleFile={this.handleFile}
            handleKeyUp={this.handleKeyUp}
            handleSubmit={this.handleSubmit}
          />
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
  selectedDatabase: string,
}

export default OnClickOutside(WriteDataForm)
