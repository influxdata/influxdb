// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Components
import {Input, Button, EmptyState} from '@influxdata/clockface'
import {Tabs, Overlay} from 'src/clockface'
import MemberList from 'src/members/components/MemberList'
import FilterList from 'src/shared/components/Filter'
import AddMembersOverlay from 'src/members/components/AddMembersOverlay'

// Actions
import {addMember, deleteMember} from 'src/members/actions'

// Types
import {User} from '@influxdata/influx'
import {IconFont, ComponentSize, ComponentColor} from '@influxdata/clockface'
import {OverlayState, AppState, Member} from 'src/types'

// API
import {client} from 'src/utils/api'

export interface UsersMap {
  [userID: string]: User
}

interface StateProps {
  members: Member[]
}

interface DispatchProps {
  onAddMember: typeof addMember
  onRemoveMember: typeof deleteMember
}

type Props = StateProps & DispatchProps

interface State {
  searchTerm: string
  overlayState: OverlayState
  users: UsersMap
}

class Members extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
      overlayState: OverlayState.Closed,
      users: {},
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
        <FilterList<Member>
          list={this.props.members}
          searchKeys={['name']}
          searchTerm={searchTerm}
        >
          {ms => (
            <MemberList
              members={ms}
              emptyState={this.emptyState}
              onDelete={this.removeMember}
            />
          )}
        </FilterList>
        <Overlay visible={overlayState === OverlayState.Open}>
          <AddMembersOverlay
            onCloseModal={this.handleCloseModal}
            users={this.state.users}
            addMember={this.addMember}
          />
        </Overlay>
      </>
    )
  }

  private removeMember = (member: Member) => {
    const {onRemoveMember} = this.props
    onRemoveMember(member)
  }

  private addMember = (member: Member) => {
    const {onAddMember} = this.props
    onAddMember(member)
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
    const {members} = this.props
    const apiUsers = await client.users.getAll()
    const allUsers = apiUsers.reduce((acc, u) => _.set(acc, u.id, u), {})
    const users = _.omit(allUsers, members.map(m => m.id))

    this.setState({users})
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`Looks like there aren't any Members , why not invite some?`}
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

const mstp = ({members: {list}}: AppState): StateProps => {
  return {members: list}
}

const mdtp: DispatchProps = {
  onAddMember: addMember,
  onRemoveMember: deleteMember,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(Members)
