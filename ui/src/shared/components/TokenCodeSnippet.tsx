// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  IconFont,
} from '@influxdata/clockface'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Notification} from 'src/types'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import CopyButton from 'src/shared/components/CopyButton'

// Actions
import {generateTelegrafToken} from 'src/dataLoaders/actions/dataLoaders'

export interface Props {
  configID: string
  copyText: string
  onCopyText?: (text: string, status: boolean) => Notification
  testID?: string
  label: string
}

interface DispatchProps {
  onGenerateTelegrafToken: typeof generateTelegrafToken
}

@ErrorHandling
class TokenCodeSnippet extends PureComponent<Props & DispatchProps> {
  public static defaultProps = {
    label: 'Code Snippet',
  }

  public handleRefreshClick = () => {
    const {configID, onGenerateTelegrafToken} = this.props
    onGenerateTelegrafToken(configID)
  }

  public render() {
    const {copyText, label, onCopyText} = this.props
    const testID = this.props.testID || 'code-snippet'

    return (
      <div className="code-snippet" data-testid={testID}>
        <FancyScrollbar
          autoHide={false}
          autoHeight={true}
          maxHeight={400}
          className="code-snippet--scroll"
        >
          <div className="code-snippet--text">
            <pre>
              export INFLUX_TOKEN=<i>{copyText || '<INFLUX_TOKEN>'}</i>
            </pre>
          </div>
        </FancyScrollbar>
        <div className="code-snippet--footer">
          <div>
            <CopyButton
              textToCopy={copyText}
              onCopyText={onCopyText}
              contentName="Script"
            />
            <Button
              size={ComponentSize.ExtraSmall}
              status={
                copyText === ''
                  ? ComponentStatus.Default
                  : ComponentStatus.Disabled
              }
              text="Generate New Token"
              titleText="Generate New Token"
              icon={IconFont.Refresh}
              color={ComponentColor.Success}
              onClick={this.handleRefreshClick}
              className="new-token--btn"
            />
          </div>
          <label className="code-snippet--label">{label}</label>
        </div>
      </div>
    )
  }
}

const mdtp: DispatchProps = {
  onGenerateTelegrafToken: generateTelegrafToken,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(TokenCodeSnippet)
