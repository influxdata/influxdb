import React, {PureComponent, ChangeEvent, RefObject} from 'react'
import classnames from 'classnames'

// Components
import DragAndDropHeader from './DragAndDropHeader'
import DragAndDropButtons from './DragAndDropButtons'
import DragInfo from './DragInfo'

export const MAX_FILE_SIZE = 1e7

interface Props {
  className: string
  onSubmit: () => void
  onSetBody: (body: string) => void
}

interface State {
  fileSize: number
  inputContent: string
  uploadContent: string
  fileName: string
  dragClass: string
}

let dragCounter = 0
class DragAndDrop extends PureComponent<Props, State> {
  private fileInput: RefObject<HTMLInputElement>

  constructor(props: Props) {
    super(props)

    this.fileInput = React.createRef()

    this.state = {
      fileSize: -Infinity,
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
    const {uploadContent, fileName, fileSize} = this.state

    return (
      <div className={this.containerClass}>
        <div className={this.dragAreaClass} onClick={this.handleFileOpen}>
          <DragAndDropHeader
            uploadContent={uploadContent}
            fileName={fileName}
            fileSize={fileSize}
          />
          <DragInfo uploadContent={uploadContent} />
          <input
            type="file"
            data-testid="drag-and-drop--input"
            ref={this.fileInput}
            className="drag-and-drop--input"
            accept="*"
            onChange={this.handleFileClick}
          />
          <DragAndDropButtons
            onSubmit={this.handleSubmit}
            fileSize={fileSize}
            onCancel={this.handleCancelFile}
            uploadContent={uploadContent}
          />
        </div>
      </div>
    )
  }

  private handleSubmit = () => {
    this.props.onSubmit()
  }

  private handleWindowDragOver = (event: DragEvent) => {
    event.preventDefault()
  }

  private get containerClass(): string {
    const {dragClass} = this.state
    const {className} = this.props

    return classnames('drag-and-drop', {
      [dragClass]: true,
      [className]: className,
    })
  }

  private get dragAreaClass(): string {
    const {uploadContent} = this.state

    return classnames('drag-and-drop--form', {active: !uploadContent})
  }

  private handleFileClick = (e: ChangeEvent<HTMLInputElement>): void => {
    const file: File = e.currentTarget.files[0]
    const fileSize = file.size

    this.setState({
      fileName: file.name,
      fileSize,
    })

    if (fileSize >= MAX_FILE_SIZE) {
      return
    }

    e.preventDefault()
    e.stopPropagation()

    this.uploadFile(file)
  }

  private handleFileDrop = (e: DragEvent): void => {
    const file: File = e.dataTransfer.files[0]
    const fileSize = file.size

    this.setState({
      fileName: file.name,
      fileSize: file.size,
      dragClass: 'drag-none',
    })

    if (fileSize >= MAX_FILE_SIZE) {
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
          uploadContent: reader.result as string,
          fileName: file.name,
        },
        () => {
          this.props.onSetBody(this.state.uploadContent)
        }
      )
    }
    reader.readAsText(file)
  }

  private handleFileOpen = (): void => {
    const {uploadContent} = this.state
    if (uploadContent === '') {
      this.fileInput.current.click()
    }
  }

  private handleCancelFile = (): void => {
    this.setState({uploadContent: '', fileSize: -Infinity})
    this.fileInput.current.value = ''
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
