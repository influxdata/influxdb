import React, {
  PureComponent,
  DragEvent,
  ChangeEvent,
  KeyboardEvent,
} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import OnClickOutside from 'src/shared/components/OnClickOutside'
import WriteDataBody from 'src/data_explorer/components/WriteDataBody'
import WriteDataHeader from 'src/data_explorer/components/WriteDataHeader'

import {OVERLAY_TECHNOLOGY} from 'src/shared/constants/classNames'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Source, DropdownItem} from 'src/types'
import {RadioButton} from 'src/reusable_ui/components/radio_buttons/RadioButtons'

let dragCounter = 0
const writeDataModes = [{text: 'File Upload'}, {text: 'Manual Entry'}]

interface Props {
  source: Source
  selectedDatabase: string
  onClose: () => void
  errorThrown: () => void
  writeLineProtocol: (source: Source, database: string, content: string) => void
}

interface State {
  selectedDatabase: string
  inputContent: string | null
  uploadContent: string
  fileName: string
  progress: string
  mode: RadioButton
  dragClass: string
  isUploading: boolean
}

@ErrorHandling
class WriteDataForm extends PureComponent<Props, State> {
  private fileInput: HTMLInputElement

  constructor(props) {
    super(props)
    this.state = {
      selectedDatabase: props.selectedDatabase,
      inputContent: null,
      uploadContent: '',
      fileName: '',
      progress: '',
      mode: writeDataModes[0],
      dragClass: 'drag-none',
      isUploading: false,
    }
  }

  public render() {
    const {onClose, errorThrown, source} = this.props
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
            source={source}
            onClose={onClose}
            modes={writeDataModes}
            errorThrown={errorThrown}
            onToggleMode={this.handleToggleMode}
            handleSelectDatabase={this.handleSelectDatabase}
          />
          <WriteDataBody
            {...this.state}
            modes={writeDataModes}
            fileInput={this.handleFileInputRef}
            handleEdit={this.handleEdit}
            handleFile={this.handleFile}
            handleKeyUp={this.handleKeyUp}
            handleSubmit={this.handleSubmit}
            handleFileOpen={this.handleFileOpen}
            handleCancelFile={this.handleCancelFile}
          />
        </div>
      </div>
    )
  }

  private handleToggleMode = (mode: RadioButton) => {
    this.setState({mode})
  }

  private handleSelectDatabase = (item: DropdownItem): void => {
    this.setState({selectedDatabase: item.text})
  }

  private handleKeyUp = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    e.stopPropagation()
    if (e.key === 'Escape') {
      const {onClose} = this.props
      onClose()
    }
  }

  private handleSubmit = async () => {
    const {onClose, source, writeLineProtocol} = this.props
    const {inputContent, uploadContent, selectedDatabase, mode} = this.state
    let content = inputContent

    if (_.isEqual(mode, writeDataModes[0])) {
      content = uploadContent
    }
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

  private handleEdit = (e: ChangeEvent<HTMLTextAreaElement>): void => {
    this.setState({inputContent: e.target.value.trim()})
  }

  private handleFile = (drop: boolean) => (e: any): void => {
    let file
    if (drop) {
      file = e.dataTransfer.files[0]
      this.setState({
        dragClass: 'drag-none',
      })
    } else {
      file = e.currentTarget.files[0]
    }

    if (!file) {
      return
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

  private handleCancelFile = (): void => {
    this.setState({uploadContent: ''})
    this.fileInput.value = ''
  }

  private handleDragOver = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault()
    e.stopPropagation()
  }

  private handleDragEnter = (e: DragEvent<HTMLDivElement>): void => {
    dragCounter += 1
    e.preventDefault()
    this.setState({dragClass: 'drag-over'})
  }

  private handleDragLeave = (e: DragEvent<HTMLDivElement>): void => {
    dragCounter -= 1
    e.preventDefault()
    if (dragCounter === 0) {
      this.setState({dragClass: 'drag-none'})
    }
  }

  private handleFileOpen = (): void => {
    const {uploadContent} = this.state
    if (uploadContent === '') {
      this.fileInput.click()
    }
  }

  private handleFileInputRef = (r: HTMLInputElement) => (this.fileInput = r)
}

export default OnClickOutside(WriteDataForm)
