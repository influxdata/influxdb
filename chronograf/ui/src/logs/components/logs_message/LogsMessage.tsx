import React, {PureComponent, MouseEvent} from 'react'

import CopyToClipboard from 'react-copy-to-clipboard'

import {
  copyToClipboardSuccess,
  copyToClipboardFailed,
} from 'src/shared/copy/notifications'
import {Button, IconFont, ComponentColor, ComponentSize} from 'src/clockface'

import {NotificationAction} from 'src/types'

interface Props {
  formattedValue: string
  notify: NotificationAction
  searchPattern?: string
}

class LogsMessage extends PureComponent<Props> {
  public render() {
    const {formattedValue} = this.props

    return (
      <div className="logs-message">
        {this.props.formattedValue}
        <CopyToClipboard text={formattedValue} onCopy={this.handleCopyAttempt}>
          <Button
            size={ComponentSize.ExtraSmall}
            color={ComponentColor.Primary}
            customClass="logs-message--copy"
            titleText="copy to clipboard"
            icon={IconFont.Duplicate}
            text="Copy"
            onClick={this.handleClickCopy}
          />
        </CopyToClipboard>
      </div>
    )
  }

  private handleCopyAttempt = (
    copiedText: string,
    isSuccessful: boolean
  ): void => {
    const {notify} = this.props
    const text = copiedText.slice(0, 20).trimRight()
    const truncatedText = `${text}...`
    const title = 'Log message '

    if (isSuccessful) {
      notify(copyToClipboardSuccess(truncatedText, title))
    } else {
      notify(copyToClipboardFailed(truncatedText, title))
    }
  }

  private handleClickCopy(e: MouseEvent<HTMLButtonElement>) {
    e.stopPropagation()
    e.preventDefault()
  }
}

export default LogsMessage
