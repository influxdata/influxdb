import React, {PureComponent, ReactElement} from 'react'
import classnames from 'classnames'

interface Props {
  fileTypesToAccept?: string
  containerClass?: string
  handleSubmit: (uploadContent: string, fileName: string) => void
  submitText?: string
  submitOnDrop?: boolean
  submitOnUpload?: boolean
  compact?: boolean
}

interface State {
  inputContent: string | null
  uploadContent: string
  fileName: string
  dragClass: string
}

let dragCounter = 0
class DragAndDrop extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    submitText: 'Write this File',
    submitOnDrop: false,
    submitOnUpload: false,
    compact: false,
  }

  private fileInput: HTMLInputElement

  constructor(props: Props) {
    super(props)

    this.state = {
      inputContent: null,
      uploadContent: '',
      fileName: '',
      dragClass: 'drag-none',
    }
  }

  public componentDidMount() {
    window.addEventListener('dragover', this.handleWindowDragOver)
    window.addEventListener('drop', this.handleFileDrop)
    window.addEventListener('dragenter', this.handleDragEnter)
    window.addEventListener('dragleave', this.handleDragLeave)
  }

  public componentWillUnmount() {
    window.removeEventListener('dragover', this.handleWindowDragOver)
    window.removeEventListener('drop', this.handleFileDrop)
    window.removeEventListener('dragenter', this.handleDragEnter)
    window.removeEventListener('dragleave', this.handleDragLeave)
  }

  public render() {
    return (
      <div className={this.containerClass}>
        <div className={this.dragAreaClass} onClick={this.handleFileOpen}>
          {this.dragAreaHeader}
          <div className={this.infoClass} />
          <input
            type="file"
            ref={r => (this.fileInput = r)}
            className="drag-and-drop--input"
            accept={this.fileTypesToAccept}
            onChange={this.handleFileClick}
          />
          {this.buttons}
        </div>
      </div>
    )
  }

  private handleWindowDragOver = (event: DragEvent) => {
    event.preventDefault()
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
    const {compact} = this.props

    return classnames('drag-and-drop', {compact, [dragClass]: true})
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
    const {submitText, submitOnDrop} = this.props

    if (!uploadContent) {
      return null
    }

    if (submitOnDrop) {
      return (
        <span className="drag-and-drop--buttons">
          <button
            className="btn btn-sm btn-default"
            onClick={this.handleCancelFile}
          >
            Cancel
          </button>
        </span>
      )
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
    const {uploadContent, fileName} = this.state

    handleSubmit(uploadContent, fileName)
  }

  private handleFileClick = (e: any): void => {
    const file = e.currentTarget.files[0]

    if (!file) {
      return
    }

    e.preventDefault()
    e.stopPropagation()

    const reader = new FileReader()
    reader.readAsText(file)
    reader.onload = loadEvent => {
      this.setState(
        {
          uploadContent: loadEvent.target.result,
          fileName: file.name,
        },
        this.submitOnUpload
      )
    }
  }

  private handleFileDrop = (e: any): void => {
    const file = e.dataTransfer.files[0]
    this.setState({
      dragClass: 'drag-none',
    })

    if (!file) {
      return
    }

    e.preventDefault()
    e.stopPropagation()

    const reader = new FileReader()
    reader.readAsText(file)
    reader.onload = loadEvent => {
      this.setState(
        {
          uploadContent: loadEvent.target.result,
          fileName: file.name,
        },
        this.submitOnDrop
      )
    }
  }

  private submitOnDrop() {
    const {submitOnDrop} = this.props
    if (submitOnDrop) {
      this.handleSubmit()
    }
  }

  private submitOnUpload() {
    const {submitOnUpload} = this.props
    if (submitOnUpload) {
      this.handleSubmit()
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

  private handleDragEnter = (e: DragEvent): void => {
    dragCounter += 1
    e.preventDefault()
    this.setState({dragClass: 'drag-over'})
  }

  private handleDragLeave = (e: DragEvent): void => {
    dragCounter -= 1
    e.preventDefault()
    if (dragCounter === 0) {
      this.setState({dragClass: 'drag-none'})
    }
  }
}

export default DragAndDrop
