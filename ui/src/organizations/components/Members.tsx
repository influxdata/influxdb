// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import {
  ComponentSize,
  EmptyState,
  IconFont,
  Input,
  Tabs,
  OverlayTechnology,
} from 'src/clockface'
import MemberList from 'src/organizations/components/MemberList'
import FilterList from 'src/shared/components/Filter'
import AddMembersOverlay from 'src/organizations/components/AddMembersOverlay'

// Actions
import * as NotificationsActions from 'src/types/actions/notifications'

// Types
import {
  ResourceOwner,
  User,
  AddResourceMemberRequestBody,
} from '@influxdata/influx'
import {OverlayState} from 'src/types'
import {Button, ComponentColor} from '@influxdata/clockface'

// APIs
import {client} from 'src/utils/api'
import {
  memberAddSuccess,
  memberAddFailed,
} from 'src/shared/copy/v2/notifications'

interface Props {
  members: ResourceOwner[]
  orgName: string
  orgID: string
  notify: NotificationsActions.PublishNotificationActionCreator
}

interface State {
  searchTerm: string
  overlayState: OverlayState
  users: User[]
}

export default class Members extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
      overlayState: OverlayState.Closed,
      users: [],
    }
  }
  public render() {
    const {searchTerm, overlayState} = this.state

    return (
      <>
        <Tabs.TabContentsHeader>
          <Input
            icon={IconFont.Search}
            placeholder="Filter members..."
            widthPixels={290}
            value={searchTerm}
            onChange={this.handleFilterChange}
            onBlur={this.handleFilterChange}
          />
          <Button
            text="Add Member"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
            onClick={this.handleOpenModal}
          />
        </Tabs.TabContentsHeader>
        <FilterList<ResourceOwner>
          list={this.props.members}
          searchKeys={['name']}
          searchTerm={searchTerm}
        >
          {ms => <MemberList members={ms} emptyState={this.emptyState} />}
        </FilterList>
        <OverlayTechnology visible={overlayState === OverlayState.Open}>
          <AddMembersOverlay
            onCloseModal={this.handleCloseModal}
            users={this.state.users}
            addUser={this.addUser}
          />
        </OverlayTechnology>
      </>
    )
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleOpenModal = async () => {
    await this.getUsers()
    this.setState({overlayState: OverlayState.Open})
  }

  private handleCloseModal = (): void => {
    this.setState({overlayState: OverlayState.Closed})
  }

  private async getUsers() {
    const data = await client.users.getAllUsers()
    this.setState({users: data.users})
  }

  private addUser = async (user: AddResourceMemberRequestBody) => {
    const {notify} = this.props

    try {
      await client.organizations.addMember(this.props.orgID, user)
      this.setState({overlayState: OverlayState.Closed})
      notify(memberAddSuccess())
    } catch (e) {
      console.error(e)
      this.setState({overlayState: OverlayState.Closed})
      const message = _.get(e, 'response.data.message', 'Unknown error')
      notify(memberAddFailed(message))
    }
  }

  private get emptyState(): JSX.Element {
    const {orgName} = this.props
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`${orgName} doesn't have any Members , why not invite some?`}
            highlightWords={['Members']}
          />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Members match your query" />
      </EmptyState>
    )
  }
}
