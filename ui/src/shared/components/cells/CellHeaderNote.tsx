// Libraries
import React, {FunctionComponent, useRef, RefObject, useState} from 'react'
import classnames from 'classnames'

// Components
import {
  Popover,
  PopoverInteraction,
  Icon,
  IconFont,
  Appearance,
  DapperScrollbars,
} from '@influxdata/clockface'

import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'

// Constants
const MAX_POPOVER_WIDTH = 280
const MAX_POPOVER_HEIGHT = 200

interface Props {
  note: string
}

const CellHeaderNote: FunctionComponent<Props> = ({note}) => {
  const [popoverVisible, setPopoverVisibility] = useState<boolean>(false)
  const triggerRef: RefObject<HTMLDivElement> = useRef<HTMLDivElement>(null)
  const indicatorClass = classnames('cell--note-indicator', {
    'cell--note-indicator__active': popoverVisible,
  })
  const contentStyle = {
    width: `${MAX_POPOVER_WIDTH}px`,
    minWidth: `${MAX_POPOVER_WIDTH}px`,
    maxWidth: `${MAX_POPOVER_WIDTH}px`,
    maxHeight: `${MAX_POPOVER_HEIGHT}px`,
  }

  const handlePopoverShow = (): void => {
    setPopoverVisibility(true)
  }

  const handlePopoverHide = (): void => {
    setPopoverVisibility(false)
  }

  return (
    <>
      <div className={indicatorClass} ref={triggerRef}>
        <Icon glyph={IconFont.Chat} />
      </div>
      <Popover
        triggerRef={triggerRef}
        appearance={Appearance.Outline}
        showEvent={PopoverInteraction.Click}
        hideEvent={PopoverInteraction.Click}
        onShow={handlePopoverShow}
        onHide={handlePopoverHide}
        enableDefaultStyles={false}
        contents={() => (
          <DapperScrollbars style={contentStyle} autoSize={true}>
            <div className="cell--note-contents markdown-format">
              <MarkdownRenderer text={note} />
            </div>
          </DapperScrollbars>
        )}
      />
    </>
  )
}

export default CellHeaderNote
