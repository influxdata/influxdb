import React, {PureComponent, ReactElement, DragEvent} from 'react'

interface Props {
  fileTypesToAccept?: string
  containerClass?: string
  handleSubmit: (uploadContent: string) => void
}

interface State {
  inputContent: string | null
  uploadContent: string
  fileName: string
  progress: string
  dragClass: string
  isUploading: boolean
}

let dragCounter = 0
class DragAndDrop extends PureComponent<Props, State> {
  private fileInput: HTMLInputElement

  constructor(props: Props) {
    super(props)

    this.state = {
      inputContent: null,
      uploadContent: '',
      fileName: '',
      progress: '',
      dragClass: 'drag-none',
      isUploading: false,
    }
  }

  public render() {
    return (
      <div className="drag-and-drop">
        {/* (Invisible, covers entire screen)
        This div handles drag only*/}
        <div
          onDrop={this.handleFile(true)}
          onDragOver={this.handleDragOver}
          onDragEnter={this.handleDragEnter}
          onDragExit={this.handleDragLeave}
          onDragLeave={this.handleDragLeave}
          className="drag-and-drop--dropzone"
        />
        {/* visible form, handles drag & click */}
        {this.dragArea}
      </div>
    )
  }

  private get dragArea(): ReactElement<HTMLDivElement> {
    return (
      <div
        className={this.dragAreaClass}
        onClick={this.handleFileOpen}
        onDrop={this.handleFile(true)}
        onDragOver={this.handleDragOver}
        onDragEnter={this.handleDragEnter}
        onDragExit={this.handleDragLeave}
        onDragLeave={this.handleDragLeave}
      >
        {this.dragAreaHeader}
        <div className={this.infoClass} />
        <input
          type="file"
          ref={r => (this.fileInput = r)}
          className="write-data-form--upload"
          accept={this.fileTypesToAccept}
          onChange={this.handleFile(false)}
        />
        {this.buttons}
      </div>
    )
  }

  private get fileTypesToAccept(): string {
    const {fileTypesToAccept} = this.props

    if (!fileTypesToAccept) {
      return '*'
    }

    return fileTypesToAccept
  }

  private get infoClass(): string {
    const {uploadContent} = this.state

    if (uploadContent) {
      return 'write-data-form--graphic write-data-form--graphic_success'
    }

    return 'write-data-form--graphic'
  }

  private get dragAreaClass(): string {
    const {uploadContent} = this.state

    if (uploadContent) {
      return 'drag-and-drop--form'
    }

    return 'drag-and-drop--form active'
  }

  private get dragAreaHeader(): ReactElement<HTMLHeadElement> {
    const {uploadContent, fileName} = this.state

    if (uploadContent) {
      return <h3 className="drag-and-drop--header selected">{fileName}</h3>
    }

    return (
      <h3 className="drag-and-drop--header empty">
        Drop a file here or click to upload
      </h3>
    )
  }

  private get buttons(): ReactElement<HTMLSpanElement> | null {
    const {uploadContent} = this.state

    if (!uploadContent) {
      return null
    }

    return (
      <span className="drag-and-drop--buttons">
        <button className="btn btn-md btn-success" onClick={this.handleSubmit}>
          Write this File
        </button>
        <button
          className="btn btn-md btn-default"
          onClick={this.handleCancelFile}
        >
          Cancel
        </button>
      </span>
    )
  }

  private handleSubmit = () => {
    const {handleSubmit} = this.props
    const {uploadContent} = this.state

    handleSubmit(uploadContent)
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

  private handleFileOpen = (): void => {
    const {uploadContent} = this.state
    if (uploadContent === '') {
      this.fileInput.click()
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
}

export default DragAndDrop
