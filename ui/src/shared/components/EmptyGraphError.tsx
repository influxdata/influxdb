// Libraries
import React, {useState, FunctionComponent} from 'react'
import CopyToClipboard from 'react-copy-to-clipboard'

// Components
import {
  Button,
  ComponentSize,
  ComponentColor,
  IconFont,
} from '@influxdata/clockface'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

interface Props {
  message: string
  testID?: string
}

const EmptyGraphError: FunctionComponent<Props> = ({message, testID}) => {
  const [didCopy, setDidCopy] = useState(false)

  const buttonText = didCopy ? 'Copied!' : 'Copy'
  const buttonColor = didCopy ? ComponentColor.Success : ComponentColor.Default

  const onClick = () => {
    setDidCopy(true)
    setTimeout(() => setDidCopy(false), 2000)
  }

  return (
    <div className="cell--view-empty" data-testid={testID}>
      <div className="empty-graph-error">
        <CopyToClipboard text={message}>
          <Button
            size={ComponentSize.ExtraSmall}
            color={buttonColor}
            titleText={buttonText}
            text={buttonText}
            onClick={onClick}
            className="empty-graph-error--copy"
          />
        </CopyToClipboard>
        <FancyScrollbar
          className="empty-graph-error--scroll"
          autoHide={false}
          thumbStartColor="#FF8564"
          thumbStopColor="#DC4E58"
        >
          <pre>
            <span
              className={`icon ${
                IconFont.AlertTriangle
              } empty-graph-error--icon`}
            />
            <code className="cell--error-message">{message}</code>
          </pre>
        </FancyScrollbar>
      </div>
    </div>
  )
}

export default EmptyGraphError
