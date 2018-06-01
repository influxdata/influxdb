import React, {
  PureComponent,
  ChangeEvent,
  KeyboardEvent,
  MouseEvent,
  DragEvent,
  ReactElement,
} from 'react'
import WriteDataFooter from 'src/data_explorer/components/WriteDataFooter'

interface Props {
  handleCancelFile: (e: MouseEvent<HTMLButtonElement>) => void
  handleEdit: (e: ChangeEvent<HTMLTextAreaElement>) => void
  handleKeyUp: (e: KeyboardEvent<HTMLTextAreaElement>) => void
  handleFile: (drop: boolean) => (e: DragEvent<HTMLInputElement>) => void
  handleSubmit: (e: MouseEvent<HTMLButtonElement>) => void
  inputContent: string
  uploadContent: string
  fileName: string
  isManual: boolean
  fileInput: (ref: any) => any
  handleFileOpen: () => void
  isUploading: boolean
}

class WriteDataBody extends PureComponent<Props> {
  public render() {
    return (
      <div className="write-data-form--body">
        {this.input}
        {this.footer}
      </div>
    )
  }

  private handleFile = (e: any): void => {
    this.props.handleFile(false)(e)
  }

  private get input(): JSX.Element {
    const {isManual} = this.props
    if (isManual) {
      return this.textarea
    }

    return this.dragArea
  }

  private get textarea(): ReactElement<HTMLTextAreaElement> {
    const {handleKeyUp, handleEdit} = this.props
    return (
      <textarea
        spellCheck={false}
        autoFocus={true}
        autoComplete="off"
        onKeyUp={handleKeyUp}
        onChange={handleEdit}
        data-test="manual-entry-field"
        className="form-control write-data-form--input"
        placeholder="<measurement>,<tag_key>=<tag_value> <field_key>=<field_value>"
      />
    )
  }

  private get dragArea(): ReactElement<HTMLDivElement> {
    const {fileInput, handleFileOpen} = this.props

    return (
      <div className={this.dragAreaClass} onClick={handleFileOpen}>
        {this.dragAreaHeader}
        <div className={this.infoClass} />
        <input
          type="file"
          ref={fileInput}
          className="write-data-form--upload"
          accept="text/*, application/gzip"
          onChange={this.handleFile}
        />
        {this.buttons}
      </div>
    )
  }

  private get dragAreaHeader(): ReactElement<HTMLHeadElement> {
    const {uploadContent, fileName} = this.props

    if (uploadContent) {
      return <h3 className="write-data-form--filepath_selected">{fileName}</h3>
    }

    return (
      <h3 className="write-data-form--filepath_empty">
        Drop a file here or click to upload
      </h3>
    )
  }

  private get infoClass(): string {
    const {uploadContent} = this.props

    if (uploadContent) {
      return 'write-data-form--graphic write-data-form--graphic_success'
    }

    return 'write-data-form--graphic'
  }

  private get buttons(): ReactElement<HTMLSpanElement> | null {
    const {uploadContent, handleSubmit, handleCancelFile} = this.props

    if (!uploadContent) {
      return null
    }

    return (
      <span className="write-data-form--file-submit">
        <button className="btn btn-md btn-success" onClick={handleSubmit}>
          Write this File
        </button>
        <button className="btn btn-md btn-default" onClick={handleCancelFile}>
          Cancel
        </button>
      </span>
    )
  }

  private get dragAreaClass(): string {
    const {uploadContent} = this.props

    if (uploadContent) {
      return 'write-data-form--file'
    }

    return 'write-data-form--file write-data-form--file_active'
  }

  private get footer(): JSX.Element | null {
    const {
      isUploading,
      isManual,
      inputContent,
      handleSubmit,
      uploadContent,
    } = this.props

    if (!isManual) {
      return null
    }

    return (
      <WriteDataFooter
        isUploading={isUploading}
        isManual={isManual}
        inputContent={inputContent}
        handleSubmit={handleSubmit}
        uploadContent={uploadContent}
      />
    )
  }
}

export default WriteDataBody
