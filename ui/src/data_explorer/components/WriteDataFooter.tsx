import React, {PureComponent} from 'react'
import {
  WRITE_DATA_DOCS_LINK,
  DATA_IMPORT_DOCS_LINK,
} from 'src/data_explorer/constants'

const submitButton = 'btn btn-sm btn-success write-data-form--submit'
const spinner = 'btn-spinner'

interface Props {
  isManual: boolean
  isUploading: boolean
  uploadContent: string
  inputContent: string
  // handleSubmit: (e: MouseEvent<HTMLButtonElement>) => void
  handleSubmit: () => void
}

class WriteDataFooter extends PureComponent<Props> {
  public render() {
    const {isManual, handleSubmit} = this.props

    return (
      <div className="write-data-form--footer">
        {isManual ? (
          <span className="write-data-form--helper">
            Need help writing InfluxDB Line Protocol? -&nbsp;
            <a href={WRITE_DATA_DOCS_LINK} target="_blank">
              See Documentation
            </a>
          </span>
        ) : (
          <span className="write-data-form--helper">
            <a href={DATA_IMPORT_DOCS_LINK} target="_blank">
              File Upload Documentation
            </a>
          </span>
        )}
        <button
          className={this.buttonClass}
          onClick={handleSubmit}
          disabled={this.buttonDisabled}
          data-test="write-data-submit-button"
        >
          Write
        </button>
      </div>
    )
  }

  get buttonDisabled(): boolean {
    const {inputContent, isManual, uploadContent, isUploading} = this.props
    return (
      (!inputContent && isManual) ||
      (!uploadContent && !isManual) ||
      isUploading
    )
  }

  get buttonClass(): string {
    const {isUploading} = this.props

    if (isUploading) {
      return `${submitButton} ${spinner}`
    }

    return submitButton
  }
}

export default WriteDataFooter
