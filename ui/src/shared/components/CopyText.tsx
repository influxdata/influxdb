// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import _ from 'lodash'
import CopyToClipboard from 'react-copy-to-clipboard'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Components
import {Button, ComponentSize, ComponentColor} from '@influxdata/clockface'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Actions
import {NotificationAction} from 'src/types'
import {
  copyToClipboardSuccess,
  copyToClipboardFailed,
} from 'src/shared/copy/notifications'

export interface Props {
  copyText: string
  notify: NotificationAction
}

@ErrorHandling
class CopyText extends PureComponent<Props> {
  public render() {
    const {copyText} = this.props
    return (
      <div className="script-snippet--container">
        <FancyScrollbar autoHide={false} autoHeight={true} maxHeight={400}>
          <p>{copyText}</p>
        </FancyScrollbar>
        <CopyToClipboard text={copyText} onCopy={this.handleCopyAttempt}>
          <Button
            customClass="copy-button"
            size={ComponentSize.Small}
            color={ComponentColor.Default}
            titleText="copy to clipboard"
            text="Copy"
            onClick={this.handleClickCopy}
          />
        </CopyToClipboard>
      </div>
    )
  }

  private handleClickCopy = (e: MouseEvent<HTMLButtonElement>) => {
    e.stopPropagation()
    e.preventDefault()
  }

  private handleCopyAttempt = (
    copiedText: string,
    isSuccessful: boolean
  ): void => {
    const {notify} = this.props
    const text = copiedText.slice(0, 30).trimRight()
    const truncatedText = `${text}...`
    const title = 'Script '

    if (isSuccessful) {
      notify(copyToClipboardSuccess(truncatedText, title))
    } else {
      notify(copyToClipboardFailed(truncatedText, title))
    }
  }
}

export default CopyText
