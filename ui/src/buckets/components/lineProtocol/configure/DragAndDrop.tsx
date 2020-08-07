import React, {PureComponent} from 'react'
import classnames from 'classnames'

// Components
import DragAndDropHeader from './DragAndDropHeader'
import DragAndDropButtons from './DragAndDropButtons'
import DragInfo from './DragInfo'
import {RemoteDataState} from '@influxdata/clockface'

interface Props {
  handleSubmit: (uploadContent: string) => void
  compact: boolean
  onCancel?: () => void
  className?: string
}

interface State {
  fileSize: number
  inputContent: string
  uploadContent: string | ArrayBuffer
  fileName: string
  dragClass: string
  readStatus: RemoteDataState
}

let dragCounter = 0
class DragAndDrop extends PureComponent<Props, State> {
  private fileInput: HTMLInputElement

  constructor(props: Props) {
    super(props)

    this.state = {
      fileSize: -Infinity,
      inputContent: null,
      uploadContent: '',
      fileName: '',
      dragClass: 'drag-none',
      readStatus: RemoteDataState.NotStarted,
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
    const {readStatus, uploadContent, fileName, fileSize} = this.state

    return (
      <div className={this.containerClass}>
        <div className={this.dragAreaClass} onClick={this.handleFileOpen}>
          <DragAndDropHeader
            uploadContent={uploadContent as string}
            readStatus={readStatus}
            fileName={fileName}
            fileSize={fileSize}
          />
          <DragInfo uploadContent={uploadContent as string} />
          <input
            type="file"
            data-testid="drag-and-drop--input"
            ref={r => (this.fileInput = r)}
            className="drag-and-drop--input"
            accept="*"
            onChange={this.handleFileClick}
          />
          <DragAndDropButtons
            onCancel={this.handleCancelFile}
            uploadContent={uploadContent as string}
          />
        </div>
      </div>
    )
  }

  private handleWindowDragOver = (event: DragEvent) => {
    event.preventDefault()
  }

  private get containerClass(): string {
    const {dragClass} = this.state
    const {compact, className} = this.props

    return classnames('drag-and-drop', {
      compact,
      [dragClass]: true,
      [className]: className,
    })
  }

  private get dragAreaClass(): string {
    const {uploadContent} = this.state

    return classnames('drag-and-drop--form', {active: !uploadContent})
  }

  private handleSubmit = () => {
    const {handleSubmit} = this.props
    const {uploadContent} = this.state

    handleSubmit(uploadContent as string)
  }

  private handleFileClick = (e: any): void => {
    const file: File = e.currentTarget.files[0]

    this.setState({
      fileName: file.name,
      fileSize: file.size,
      readStatus: RemoteDataState.Loading,
    })

    if (!file) {
      return
    }

    e.preventDefault()
    e.stopPropagation()

    this.uploadFile(file)
  }

  private handleFileDrop = (e: any): void => {
    const file: File = e.dataTransfer.files[0]
    this.setState({
      fileName: file.name,
      fileSize: file.size,
      dragClass: 'drag-none',
      readStatus: RemoteDataState.Loading,
    })

    if (!file) {
      return
    }

    e.preventDefault()
    e.stopPropagation()

    this.uploadFile(file)
  }

  private uploadFile = (file: File) => {
    const reader = new FileReader()
    reader.onload = () => {
      this.setState(
        {
          readStatus: RemoteDataState.Done,
          uploadContent: reader.result,
          fileName: file.name,
        },
        () => this.handleSubmit()
      )
    }
    reader.readAsText(file)
  }

  private handleFileOpen = (): void => {
    const {uploadContent} = this.state
    if (uploadContent === '') {
      this.fileInput.click()
    }
  }

  private handleCancelFile = (): void => {
    const {onCancel} = this.props
    this.setState({uploadContent: '', fileSize: -Infinity})
    this.fileInput.value = ''
    if (onCancel) {
      onCancel()
    }
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
