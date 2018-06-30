import React, {PureComponent, MouseEvent} from 'react'
import classnames from 'classnames'
import {WRITE_DATA_DOCS_LINK} from 'src/data_explorer/constants'

interface Props {
  isUploading: boolean
  inputContent: string
  handleSubmit: (e: MouseEvent<HTMLButtonElement>) => void
}

class WriteDataFooter extends PureComponent<Props> {
  public render() {
    const {handleSubmit} = this.props

    return (
      <div className="write-data-form--footer">
        <span className="write-data-form--helper">
          Need help writing InfluxDB Line Protocol? -&nbsp;
          <a href={WRITE_DATA_DOCS_LINK} target="_blank">
            See Documentation
          </a>
        </span>
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
    const {inputContent} = this.props

    if (inputContent) {
      return false
    }

    return true
  }

  get buttonClass(): string {
    const {isUploading} = this.props

    return classnames('btn btn-sm btn-success write-data-form--submit', {
      'btn-spinner': isUploading,
    })
  }
}

export default WriteDataFooter
