// Libraries
import React, {FunctionComponent, MouseEvent} from 'react'

// Components
import {Dropdown, ComponentColor, IconFont} from '@influxdata/clockface'

// Types
import {CheckType} from 'src/types'

interface Props {
  onCreateThreshold: () => void
  onCreateDeadman: () => void
}

const CreateCheckDropdown: FunctionComponent<Props> = ({
  onCreateThreshold,
  onCreateDeadman,
}) => {
  const handleItemClick = (type: CheckType): void => {
    if (type === 'threshold') {
      onCreateThreshold()
    }

    if (type === 'deadman') {
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
      testID="create-check"
    >
      Create
    </Dropdown.Button>
  )

  const DropdownMenu = (onCollapse: () => void) => (
    <Dropdown.Menu onCollapse={onCollapse}>
      <Dropdown.Item
        value="threshold"
        onClick={handleItemClick}
        testID="create-threshold-check"
      >
        Threshold Check
      </Dropdown.Item>
      <Dropdown.Item
        value="deadman"
        onClick={handleItemClick}
        testID="create-deadman-check"
      >
        Deadman Check
      </Dropdown.Item>
    </Dropdown.Menu>
  )

  return (
    <Dropdown
      button={DropdownButton}
      menu={DropdownMenu}
      style={{width: '164px'}}
    />
  )
}

export default CreateCheckDropdown
