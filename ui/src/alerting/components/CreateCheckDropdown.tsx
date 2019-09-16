// Libraries
import React, {FunctionComponent, MouseEvent} from 'react'

// Components
import {Dropdown, ComponentColor, IconFont} from '@influxdata/clockface'

interface Props {
  onCreateThreshold: () => void
  onCreateDeadman: () => void
}

enum CheckType {
  Deadman = 'deadman',
  Threshold = 'threshold',
}

const CreateCheckDropdown: FunctionComponent<Props> = ({
  onCreateThreshold,
  onCreateDeadman,
}) => {
  const handleItemClick = (type: CheckType): void => {
    if (type === CheckType.Threshold) {
      onCreateThreshold()
    }

    if (type === CheckType.Deadman) {
      onCreateDeadman()
    }
  }

  const DropdownButton = (
    active: boolean,
    onClick: (e: MouseEvent<HTMLButtonElement>) => void
  ) => (
    <Dropdown.Button
      icon={IconFont.Plus}
      color={ComponentColor.Primary}
      active={active}
      onClick={onClick}
    >
      Create
    </Dropdown.Button>
  )

  const DropdownMenu = (onCollapse: () => void) => (
    <Dropdown.Menu onCollapse={onCollapse}>
      <Dropdown.Item value={CheckType.Threshold} onClick={handleItemClick}>
        Threshold Check
      </Dropdown.Item>
      <Dropdown.Item value={CheckType.Deadman} onClick={handleItemClick}>
        Deadman Check
      </Dropdown.Item>
    </Dropdown.Menu>
  )

  return (
    <Dropdown button={DropdownButton} menu={DropdownMenu} widthPixels={124} />
  )
}

export default CreateCheckDropdown
