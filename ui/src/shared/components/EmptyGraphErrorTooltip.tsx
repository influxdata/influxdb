// Libraries
import React, {useState, useRef, FunctionComponent} from 'react'
import CopyToClipboard from 'react-copy-to-clipboard'

// Components
import {
  Button,
  ComponentSize,
  ComponentColor,
  IconFont,
} from '@influxdata/clockface'
import BoxTooltip from 'src/shared/components/BoxTooltip'

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

  const trigger = useRef<HTMLDivElement>(null)
  const [tooltipVisible, setTooltipVisible] = useState(false)

  let triggerRect: DOMRect = null

  if (trigger.current) {
    triggerRect = trigger.current.getBoundingClientRect() as DOMRect
  }

  return (
    <div className="cell--view-empty error" data-testid={testID}>
      <div
        onMouseEnter={() => setTooltipVisible(true)}
        onMouseLeave={() => setTooltipVisible(false)}
        ref={trigger}
      >
        <span
          className={`icon ${IconFont.AlertTriangle} empty-graph-error--icon`}
        />
        {tooltipVisible && (
          <BoxTooltip triggerRect={triggerRect} color={ComponentColor.Danger}>
            <div className="box-tooltip--contents">
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
            </div>
          </BoxTooltip>
        )}
      </div>
    </div>
  )
}

export default EmptyGraphError
