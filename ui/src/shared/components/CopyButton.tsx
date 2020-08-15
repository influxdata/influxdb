// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import CopyToClipboard from 'react-copy-to-clipboard'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {Button, ComponentColor, ComponentSize} from '@influxdata/clockface'

// Constants
import {
  copyToClipboardSuccess,
  copyToClipboardFailed,
} from 'src/shared/copy/notifications'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {Notification} from 'src/types'

interface OwnProps {
  textToCopy: string
  contentName: string // if copying a script, its "script"
  size: ComponentSize
  color: ComponentColor
  onCopyText?: (text: string, status: boolean) => Notification
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

class CopyButton extends PureComponent<Props> {
  public static defaultProps = {
    size: ComponentSize.ExtraSmall,
    color: ComponentColor.Secondary,
  }

  public render() {
    const {textToCopy, color, size} = this.props

    return (
      <CopyToClipboard text={textToCopy} onCopy={this.handleCopyAttempt}>
        <Button
          size={size}
          color={color}
          titleText="Copy to Clipboard"
          text="Copy to Clipboard"
          onClick={this.handleClickCopy}
          testID="button-copy"
        />
      </CopyToClipboard>
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
    const {notify, onCopyText} = this.props

    if (onCopyText) {
      notify(onCopyText(copiedText, isSuccessful))
      return
    }

    const {contentName} = this.props
    const text = copiedText.slice(0, 30).trimRight()
    const truncatedText = `${text}...`

    if (isSuccessful) {
      notify(copyToClipboardSuccess(truncatedText, contentName))
    } else {
      notify(copyToClipboardFailed(truncatedText, contentName))
    }
  }
}

const mdtp = {
  notify: notifyAction,
}

const connector = connect(null, mdtp)

export default connector(CopyButton)
