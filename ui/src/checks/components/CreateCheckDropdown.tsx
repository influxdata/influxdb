// Libraries
import React, {FunctionComponent, MouseEvent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {Dropdown, ComponentColor, IconFont} from '@influxdata/clockface'

// Actions
import {showOverlay, dismissOverlay} from 'src/overlays/actions/overlays'

// Types
import {CheckType} from 'src/types'
import {LimitStatus} from 'src/cloud/actions/limits'

// Constants
import {CLOUD} from 'src/shared/constants'

interface OwnProps {
  onCreateThreshold: () => void
  onCreateDeadman: () => void
  limitStatus: LimitStatus
}

type ReduxProps = ConnectedProps<typeof connector>

const CreateCheckDropdown: FunctionComponent<OwnProps & ReduxProps> = ({
  onCreateThreshold,
  onCreateDeadman,
  onDismissOverlay,
  onShowOverlay,
  limitStatus,
}) => {
  const handleItemClick = (type: CheckType): void => {
    if (CLOUD && limitStatus === LimitStatus.EXCEEDED) {
      onShowOverlay('asset-limit', {asset: 'Checks'}, onDismissOverlay)
      return
    }

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

const mdtp = {
  onShowOverlay: showOverlay,
  onDismissOverlay: dismissOverlay,
}

const connector = connect(null, mdtp)

export default connector(CreateCheckDropdown)
