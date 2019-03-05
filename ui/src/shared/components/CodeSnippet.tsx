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

// Styles
import 'src/shared/components/CodeSnippet.scss'

export interface PassedProps {
  copyText: string
  notify: NotificationAction
}

interface DefaultProps {
  label?: string
}

type Props = PassedProps & DefaultProps

@ErrorHandling
class CodeSnippet extends PureComponent<Props> {
  public static defaultProps: DefaultProps = {
    label: 'Code Snippet',
  }

  public render() {
    const {copyText, label} = this.props
    return (
      <div className="code-snippet">
        <FancyScrollbar
          autoHide={false}
          autoHeight={true}
          maxHeight={400}
          className="code-snippet--scroll"
        >
          <div className="code-snippet--text">
            <pre>
              <code>{copyText}</code>
            </pre>
          </div>
        </FancyScrollbar>
        <div className="code-snippet--footer">
          <CopyToClipboard text={copyText} onCopy={this.handleCopyAttempt}>
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Secondary}
              titleText="Copy to Clipboard"
              text="Copy to Clipboard"
              onClick={this.handleClickCopy}
            />
          </CopyToClipboard>
          <label className="code-snippet--label">{label}</label>
        </div>
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

export default CodeSnippet
