// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  IconFont,
} from '@influxdata/clockface'

// Decorator
import {Notification} from 'src/types'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import CopyButton from 'src/shared/components/CopyButton'

// Actions
import {generateTelegrafToken} from 'src/dataLoaders/actions/dataLoaders'

export interface Props {
  configID: string
  token: string
  onCopyText?: (text: string, status: boolean) => Notification
  testID?: string
  label: string
}

interface DispatchProps {
  onGenerateTelegrafToken: typeof generateTelegrafToken
}

const TokenCodeSnippet: FC<Props & DispatchProps> = ({
  token,
  onCopyText,
  testID,
  label = 'Code Snippet',
}) => {
  const handleRefreshClick = () => {
    const {configID, onGenerateTelegrafToken} = this.props
    onGenerateTelegrafToken(configID)
  }

  return (
    <div className="code-snippet" data-testid={testID}>
      <FancyScrollbar
        autoHide={false}
        autoHeight={true}
        maxHeight={400}
        className="code-snippet--scroll"
      >
        <div className="code-snippet--text">
          <pre>{token}</pre>
        </div>
      </FancyScrollbar>
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
              token === '' ? ComponentStatus.Default : ComponentStatus.Disabled
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
