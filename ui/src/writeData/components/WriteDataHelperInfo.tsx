// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {
  Table,
  ComponentSize,
  ButtonShape,
  ComponentColor,
  IconFont,
  Alignment,
} from '@influxdata/clockface'
import CopyButton from 'src/shared/components/CopyButton'

// Utils
import {getOrg} from 'src/organizations/selectors'

// Types
import {AppState} from 'src/types'

type Props = ConnectedProps<typeof connector>

const WriteDataHelperInfo: FC<Props> = ({orgID, userID}) => {
  const origin = window.location.origin

  return (
    <Table fontSize={ComponentSize.Small}>
      <Table.Header>
        <Table.Row>
          <Table.HeaderCell>Info</Table.HeaderCell>
          <Table.HeaderCell />
        </Table.Row>
      </Table.Header>
      <Table.Body>
        <Table.Row>
          <Table.Cell>My User ID</Table.Cell>
          <Table.Cell horizontalAlignment={Alignment.Right}>
            <code className="write-data--helper-code">{userID}</code>
            <CopyButton
              shape={ButtonShape.Square}
              icon={IconFont.Duplicate}
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Default}
              contentName="My User ID"
              textToCopy={userID}
              testID="copy-user-id"
            />
          </Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>Organization ID</Table.Cell>
          <Table.Cell horizontalAlignment={Alignment.Right}>
            <code className="write-data--helper-code">{orgID}</code>
            <CopyButton
              shape={ButtonShape.Square}
              icon={IconFont.Duplicate}
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Default}
              contentName="Organization ID"
              textToCopy={orgID}
              testID="copy-org-id"
            />
          </Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>Host URL</Table.Cell>
          <Table.Cell horizontalAlignment={Alignment.Right}>
            <code className="write-data--helper-code">{origin}</code>
            <CopyButton
              shape={ButtonShape.Square}
              icon={IconFont.Duplicate}
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Default}
              contentName="Host URL"
              textToCopy={origin}
              testID="copy-host"
            />
          </Table.Cell>
        </Table.Row>
      </Table.Body>
    </Table>
  )
}

const mstp = (state: AppState) => {
  const {id} = getOrg(state)

  return {
    orgID: id,
    userID: state.me.id,
  }
}

const connector = connect(mstp)

export default connector(WriteDataHelperInfo)
