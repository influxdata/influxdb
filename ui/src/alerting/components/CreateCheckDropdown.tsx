// Libraries
import React, {FunctionComponent, MouseEvent} from 'react'

// Components
import {Dropdown, ComponentColor, IconFont} from '@influxdata/clockface'
import OverlayLink from 'src/overlays/components/OverlayLink'

interface Props {}

const CreateCheckDropdown: FunctionComponent<Props> = () => {
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
      <OverlayLink overlayID="create-threshold-check">
        {onClick => (
          <Dropdown.Item
            value="threshold"
            onClick={onClick}
            testID="create-threshold-check"
          >
            Threshold Check
          </Dropdown.Item>
        )}
      </OverlayLink>
      <OverlayLink overlayID="create-deadman-check">
        {onClick => (
          <Dropdown.Item
            value="deadman"
            onClick={onClick}
            testID="create-deadman-check"
          >
            Deadman Check
          </Dropdown.Item>
        )}
      </OverlayLink>
    </Dropdown.Menu>
  )

  return (
    <Dropdown button={DropdownButton} menu={DropdownMenu} widthPixels={124} />
  )
}

export default CreateCheckDropdown
