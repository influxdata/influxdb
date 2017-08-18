import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'

import OnClickOutside from 'shared/components/OnClickOutside'
import WriteDataBody from 'src/data_explorer/components/WriteDataBody'
import WriteDataHeader from 'src/data_explorer/components/WriteDataHeader'

import {OVERLAY_TECHNOLOGY} from 'shared/constants/classNames'
let dragCounter = 0

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
      isUploading: false,
    }
  }

  toggleWriteView = isManual => () => {
    this.setState({isManual})
  }

  handleSelectDatabase = item => {
    this.setState({selectedDatabase: item.text})
  }

  handleClickOutside = e => {
    // guard against clicking to close error notification
    if (e.target.className === OVERLAY_TECHNOLOGY) {
      const {onClose} = this.props
      onClose()
    }
  }

  handleKeyUp = e => {
    e.stopPropagation()
    if (e.key === 'Escape') {
      const {onClose} = this.props
      onClose()
    }
  }

  handleSubmit = async () => {
    const {onClose, source, writeLineProtocol} = this.props
    const {inputContent, uploadContent, selectedDatabase, isManual} = this.state
    const content = isManual ? inputContent : uploadContent
    this.setState({isUploading: true})

    try {
      await writeLineProtocol(source, selectedDatabase, content)
      this.setState({isUploading: false})
      onClose()
      window.location.reload()
    } catch (error) {
      this.setState({isUploading: false})
      console.error(error.data.error)
    }
  }

  handleEdit = e => {
    this.setState({inputContent: e.target.value.trim()})
  }

  handleFile = drop => e => {
    let file
    if (drop) {
      file = e.dataTransfer.files[0]
      this.setState({
        dragClass: 'drag-none',
      })
    } else {
      file = e.target.files[0]
    }

    e.preventDefault()
    e.stopPropagation()

    const reader = new FileReader()
    reader.readAsText(file)
    reader.onload = loadEvent => {
      this.setState({
        uploadContent: loadEvent.target.result,
        fileName: file.name,
      })
    }
  }

  handleDragOver = e => {
    e.preventDefault()
    e.stopPropagation()
  }

  handleDragEnter = e => {
    dragCounter += 1
    e.preventDefault()
    this.setState({dragClass: 'drag-over'})
  }

  handleDragLeave = e => {
    dragCounter -= 1
    e.preventDefault()
    if (dragCounter === 0) {
      this.setState({dragClass: 'drag-none'})
    }
  }

  handleFileOpen = () => {
    this.fileInput.click()
  }

  handleFileInputRef = el => (this.fileInput = el)

  render() {
    const {onClose, errorThrown} = this.props
    const {dragClass} = this.state

    return (
      <div
        onDrop={this.handleFile(true)}
        onDragOver={this.handleDragOver}
        onDragEnter={this.handleDragEnter}
        onDragExit={this.handleDragLeave}
        onDragLeave={this.handleDragLeave}
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
            fileInput={this.handleFileInputRef}
            handleEdit={this.handleEdit}
            handleFile={this.handleFile}
            handleKeyUp={this.handleKeyUp}
            handleSubmit={this.handleSubmit}
            handleFileOpen={this.handleFileOpen}
          />
        </div>
        <div className="write-data-form--drag-here">
          Drag & Drop a File to Upload
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
