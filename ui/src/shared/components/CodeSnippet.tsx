// Libraries
import React, {PureComponent} from 'react'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Notification} from 'src/types'

// Components
import {DapperScrollbars} from '@influxdata/clockface'
import CopyButton from 'src/shared/components/CopyButton'

export interface Props {
  copyText: string
  onCopyText?: (text?: string, status?: boolean) => Notification
  testID?: string
  label: string
  onClick?: () => void
}

@ErrorHandling
class CodeSnippet extends PureComponent<Props> {
  public static defaultProps = {
    label: 'Code Snippet',
  }

  public render() {
    const {copyText, label, onCopyText, onClick} = this.props
    const testID = this.props.testID || 'code-snippet'

    return (
      <div className="code-snippet" data-testid={testID}>
        <DapperScrollbars
          autoHide={false}
          autoSizeHeight={true}
          className="code-snippet--scroll"
        >
          <div className="code-snippet--text">
            <pre>
              <code>{copyText}</code>
            </pre>
          </div>
        </DapperScrollbars>
        <div className="code-snippet--footer">
          <CopyButton
            textToCopy={copyText}
            onCopyText={onCopyText}
            contentName="Script"
            onClick={onClick}
          />
          <label className="code-snippet--label">{label}</label>
        </div>
      </div>
    )
  }
}

export default CodeSnippet
