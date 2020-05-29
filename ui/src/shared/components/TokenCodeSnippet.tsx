// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  IconFont,
  DapperScrollbars,
} from '@influxdata/clockface'

// Decorator
import {Notification} from 'src/types'

// Components
import CopyButton from 'src/shared/components/CopyButton'

// Actions
import {generateTelegrafToken} from 'src/dataLoaders/actions/dataLoaders'

export interface Props {
  configID: string
  label: string
  onCopyText?: (text: string, status: boolean) => Notification
  onGenerateTelegrafToken: typeof generateTelegrafToken
  testID?: string
  token: string
}

interface DispatchProps {
  onGenerateTelegrafToken: typeof generateTelegrafToken
}

const TokenCodeSnippet: FC<Props & DispatchProps> = ({
  configID,
  onCopyText,
  label = 'Code Snippet',
  testID,
  token,
  onGenerateTelegrafToken,
}) => {
  const handleRefreshClick = () => {
    onGenerateTelegrafToken(configID)
  }

  return (
    <div className="code-snippet" data-testid={testID}>
      <DapperScrollbars
        autoHide={false}
        autoSizeHeight={true}
        className="code-snippet--scroll"
      >
        <div className="code-snippet--text">
          <pre>{token}</pre>
        </div>
      </DapperScrollbars>
      <div className="code-snippet--footer">
        <div>
          <CopyButton
            textToCopy={token}
            onCopyText={onCopyText}
            contentName="Script"
          />
          <Button
            size={ComponentSize.ExtraSmall}
            status={
              token.includes('<INFLUX_TOKEN>')
                ? ComponentStatus.Default
                : ComponentStatus.Disabled
            }
            text="Generate New Token"
            titleText="Generate New Token"
            icon={IconFont.Refresh}
            color={ComponentColor.Success}
            onClick={handleRefreshClick}
            className="new-token--btn"
          />
        </div>
        <label className="code-snippet--label">{label}</label>
      </div>
    </div>
  )
}

const mdtp: DispatchProps = {
  onGenerateTelegrafToken: generateTelegrafToken,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(TokenCodeSnippet)
