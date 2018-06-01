import React, {PureComponent, ReactElement, DragEvent} from 'react'
import classnames from 'classnames'

interface Props {
  fileTypesToAccept?: string
  containerClass?: string
  handleSubmit: (uploadContent: string) => void
  submitText?: string
}

interface State {
  inputContent: string | null
  uploadContent: string
  fileName: string
  progress: string
  dragClass: string
}

let dragCounter = 0
class DragAndDrop extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    submitText: 'Write this File',
  }

  private fileInput: HTMLInputElement

  constructor(props: Props) {
    super(props)

    this.state = {
      inputContent: null,
      uploadContent: '',
      fileName: '',
      progress: '',
      dragClass: 'drag-none',
    }
  }

  public render() {
    return (
      <div className={this.containerClass}>
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
          className="drag-and-drop--input"
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

  private get containerClass(): string {
    const {dragClass} = this.state

    return `drag-and-drop ${dragClass}`
  }

  private get infoClass(): string {
    const {uploadContent} = this.state

    return classnames('drag-and-drop--graphic', {success: uploadContent})
  }

  private get dragAreaClass(): string {
    const {uploadContent} = this.state

    return classnames('drag-and-drop--form', {active: !uploadContent})
  }

  private get dragAreaHeader(): ReactElement<HTMLHeadElement> {
    const {uploadContent, fileName} = this.state

    if (uploadContent) {
      return <div className="drag-and-drop--header selected">{fileName}</div>
    }

    return (
      <div className="drag-and-drop--header empty">
        Drop a file here or click to upload
      </div>
    )
  }

  private get buttons(): ReactElement<HTMLSpanElement> | null {
    const {uploadContent} = this.state
    const {submitText} = this.props

    if (!uploadContent) {
      return null
    }

    return (
      <span className="drag-and-drop--buttons">
        <button className="btn btn-sm btn-success" onClick={this.handleSubmit}>
          {submitText}
        </button>
        <button
          className="btn btn-sm btn-default"
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
