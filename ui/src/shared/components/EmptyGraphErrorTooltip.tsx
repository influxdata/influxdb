// Libraries
import React, {FC, useRef, useState} from 'react'
import CopyToClipboard from 'react-copy-to-clipboard'

// Components
import {
  Button,
  ComponentSize,
  ComponentColor,
  IconFont,
  Popover,
  PopoverPosition,
  PopoverInteraction,
  Appearance,
} from '@influxdata/clockface'

interface Props {
  message: string
  testID?: string
}

const EmptyGraphError: FC<Props> = ({message, testID}) => {
  const [didCopy, setDidCopy] = useState(false)

  const buttonText = didCopy ? 'Copied!' : 'Copy'
  const buttonColor = didCopy ? ComponentColor.Success : ComponentColor.Default

  const onClick = () => {
    setDidCopy(true)
    setTimeout(() => setDidCopy(false), 2000)
  }

  const trigger = useRef<HTMLDivElement>(null)

  return (
    <div className="cell--view-empty error" data-testid={testID}>
      <div ref={trigger}>
        <span
          className={`icon ${IconFont.AlertTriangle} empty-graph-error--icon`}
        />
        <Popover
          appearance={Appearance.Outline}
          position={PopoverPosition.ToTheRight}
          triggerRef={trigger}
          showEvent={PopoverInteraction.Hover}
          hideEvent={PopoverInteraction.Hover}
          distanceFromTrigger={8}
          testID="emptygraph-popover"
          contents={() => (
            <pre>
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
              <code>{message}</code>
            </pre>
          )}
        />
      </div>
    </div>
  )
}

export default EmptyGraphError
