// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {
  SquareButton,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  Icon,
  IconFont,
  Popover,
  PopoverInteraction,
  PopoverPosition,
  PopoverType,
} from '@influxdata/clockface'

interface Props {
  onSave: () => Promise<void>
  status: ComponentStatus
  className: string
  checkType: string
  singleField: boolean
  singleAggregateFunc: boolean
}

const CheckEOSaveButton: FunctionComponent<Props> = ({
  onSave,
  status,
  className,
  checkType,
  singleField,
  singleAggregateFunc,
}) => {
  return (
    <Popover
      visible={status !== ComponentStatus.Default}
      position={PopoverPosition.Below}
      showEvent={PopoverInteraction.None}
      hideEvent={PopoverInteraction.None}
      color={ComponentColor.Secondary}
      type={PopoverType.Outline}
      contents={() => (
        <div className="query-checklist--popover">
          <p>{`To create a ${checkType} check, your query must include:`}</p>
          <ul className="query-checklist--list">
            <QueryChecklistItem text="One field" selected={singleField} />
            {checkType === 'threshold' && (
              <QueryChecklistItem
                text="One aggregate function"
                selected={singleAggregateFunc}
              />
            )}
          </ul>
        </div>
      )}
    >
      <SquareButton
        className={className}
        icon={IconFont.Checkmark}
        color={ComponentColor.Success}
        size={ComponentSize.Small}
        status={status}
        onClick={onSave}
        testID="save-cell--button"
      />
    </Popover>
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
