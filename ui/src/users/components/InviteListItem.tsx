// Libraries
import React, {FC} from 'react'
import {capitalize} from 'lodash'

// Components
import {IndexList} from '@influxdata/clockface'
import InviteListContextMenu from './InviteListContextMenu'

// Types
import {Invite} from 'src/types'

interface Props {
  invite: Invite
}

const getDate = (datetime: string) => {
  return new Date(datetime).toLocaleDateString()
}

const InviteListItem: FC<Props> = ({invite}) => {
  const {email, role, expiresAt} = invite

  return (
    <IndexList.Row brighten={true}>
      <IndexList.Cell>
        <span className="user-list-email">{email}</span>
      </IndexList.Cell>
      {/* TODO: add back in once https://github.com/influxdata/quartz/issues/2389 back-filling of names is complete */}
      {/*<IndexList.Cell />*/}
      <IndexList.Cell className="user-list-cell-role">
        {capitalize(role)}
      </IndexList.Cell>
      <IndexList.Cell className="user-list-cell-status">
        <div>Invite expiration {getDate(expiresAt)}</div>
      </IndexList.Cell>
      <InviteListContextMenu invite={invite} />
    </IndexList.Row>
  )
}

export default InviteListItem
