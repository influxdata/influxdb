// Libraries
import React, {FunctionComponent, MouseEvent} from 'react'
import _ from 'lodash'

// Components
import {Dropdown} from '@influxdata/clockface'
import OverlayLink from 'src/overlays/components/OverlayLink'

// Types
import {IconFont, ComponentColor} from '@influxdata/clockface'

type Props = {}

const GenerateTokenDropdown: FunctionComponent<Props> = () => {
  const button = (active: boolean, onClick: (e?: MouseEvent<HTMLButtonElement>) => void) => (
    <Dropdown.Button
      active={active}
      onClick={onClick}
      icon={IconFont.Plus}
      color={ComponentColor.Primary}
      testID="dropdown-button--gen-token"
    >
      Generate
    </Dropdown.Button>
  )

  const menu = (onCollapse: () => void) => (
    <Dropdown.Menu onCollapse={onCollapse}>
      <OverlayLink overlayID="generate-read-write-token">
        {onClick => (
          <Dropdown.Item
            testID="dropdown-item generate-token--read-write"
            id="Read/Write Token"
            value="Read/Write Token"
            onClick={onClick}
          >
            Read/Write Token
          </Dropdown.Item>
        )}
      </OverlayLink>
      <OverlayLink overlayID="generate-all-access-token">
        {onClick => (
          <Dropdown.Item
            testID="dropdown-item generate-token--all-access"
            id="All Access Token"
            value="All Access Token"
            onClick={onClick}
          >
            All Access Token
        </Dropdown.Item>
        )}
      </OverlayLink>
    </Dropdown.Menu>
  )

  return (
    <Dropdown
      testID="dropdown--gen-token"
      widthPixels={160}
      button={button}
      menu={menu}
    />
  )
}

export default GenerateTokenDropdown
