// Libraries
import React, {FC, useRef, useState} from 'react'
import classnames from 'classnames'

// Components
import {
  Icon,
  IconFont,
  Popover,
  Appearance,
  PopoverInteraction,
  ComponentColor,
} from '@influxdata/clockface'

// Styles
import './LastRunTaskStatus.scss'

interface PassedProps {
  lastRunError?: string
  lastRunStatus: string
}

const LastRunTaskStatus: FC<PassedProps> = ({lastRunError, lastRunStatus}) => {
  const triggerRef = useRef<HTMLDivElement>(null)
  const [highlight, setHighlight] = useState<boolean>(false)

  let color = ComponentColor.Success
  let icon = IconFont.Checkmark
  let text = 'Task ran successfully!'

  if (lastRunStatus === 'failed' || lastRunError !== undefined) {
    color = ComponentColor.Danger
    icon = IconFont.AlertTriangle
    text = lastRunError
  }

  if (lastRunStatus === 'cancel') {
    color = ComponentColor.Warning
    icon = IconFont.Remove
    text = 'Task Cancelled'
  }

  const statusClassName = classnames('last-run-task-status', {
    [`last-run-task-status__${color}`]: color,
    'last-run-task-status__highlight': highlight,
  })

  const popoverContents = () => (
    <>
      <h6>Last Run Status:</h6>
      <p>{text}</p>
    </>
  )

  return (
    <>
      <div
        data-testid="last-run-status--icon"
        className={statusClassName}
        ref={triggerRef}
      >
        <Icon glyph={icon} />
      </div>
      <Popover
        className="last-run-task-status--popover"
        enableDefaultStyles={false}
        color={color}
        appearance={Appearance.Outline}
        triggerRef={triggerRef}
        contents={popoverContents}
        showEvent={PopoverInteraction.Hover}
        hideEvent={PopoverInteraction.Hover}
        onShow={() => setHighlight(true)}
        onHide={() => setHighlight(false)}
      />
    </>
  )
}

export default LastRunTaskStatus
