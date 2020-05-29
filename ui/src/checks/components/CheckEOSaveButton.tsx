// Libraries
import React, {FunctionComponent, useRef, RefObject} from 'react'

// Components
import {
  SquareButton,
  ButtonRef,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  Icon,
  IconFont,
  Popover,
  PopoverInteraction,
  PopoverPosition,
  Appearance,
} from '@influxdata/clockface'

interface Props {
  onSave: () => void
  status: ComponentStatus
  className: string
  checkType: string
  singleField: boolean
  singleAggregateFunc: boolean
  oneOrMoreThresholds: boolean
}

const CheckEOSaveButton: FunctionComponent<Props> = ({
  onSave,
  status,
  className,
  checkType,
  singleField,
  singleAggregateFunc,
  oneOrMoreThresholds,
}) => {
  const triggerRef: RefObject<ButtonRef> = useRef(null)

  return (
    <>
      <Popover
        triggerRef={triggerRef}
        visible={status !== ComponentStatus.Default}
        position={PopoverPosition.Below}
        enableDefaultStyles={false}
        showEvent={PopoverInteraction.None}
        hideEvent={PopoverInteraction.None}
        color={ComponentColor.Secondary}
        appearance={Appearance.Outline}
        contents={() => (
          <div className="query-checklist--popover">
            <p>{`To create a ${checkType} check, you must select:`}</p>
            <ul className="query-checklist--list">
              <QueryChecklistItem text="One field" selected={singleField} />
              {checkType === 'threshold' && (
                <>
                  <QueryChecklistItem
                    text="One aggregate function"
                    selected={singleAggregateFunc}
                  />
                  <QueryChecklistItem
                    text="One or more thresholds"
                    selected={oneOrMoreThresholds}
                  />
                </>
              )}
            </ul>
          </div>
        )}
      />
      <SquareButton
        ref={triggerRef}
        className={className}
        icon={IconFont.Checkmark}
        color={ComponentColor.Success}
        size={ComponentSize.Small}
        status={status}
        onClick={onSave}
        testID="save-cell--button"
      />
    </>
  )
}

export default CheckEOSaveButton

interface ChecklistItemProps {
  selected: boolean
  text: string
}

const QueryChecklistItem: FunctionComponent<ChecklistItemProps> = ({
  selected,
  text,
}) => {
  const className = selected
    ? 'query-checklist--item valid'
    : 'query-checklist--item error'
  const icon = selected ? IconFont.Checkmark : IconFont.Remove

  return (
    <li className={className}>
      <Icon glyph={icon} />
      {text}
    </li>
  )
}
