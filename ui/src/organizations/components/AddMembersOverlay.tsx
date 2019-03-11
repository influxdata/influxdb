// Libraries
import React, {PureComponent} from 'react'

// Components
import {Overlay} from 'src/clockface'
import AddMembersForm from './AddMembersForm'
import {AddResourceMemberRequestBody} from '@influxdata/influx'

// Types
import {UsersMap} from 'src/organizations/components/Members'

interface Props {
  onCloseModal: () => void
  users: UsersMap
  addMember: (user: AddResourceMemberRequestBody) => void
}

interface State {
  selectedUserIDs: string[]
}

export default class AddMembersOverlay extends PureComponent<Props, State> {
  public state: State = {
    selectedUserIDs: [],
  }

  constructor(props) {
    super(props)
  }

  public render() {
    const {onCloseModal, users} = this.props
    const {selectedUserIDs} = this.state

    return (
      <Overlay.Container maxWidth={500}>
        <Overlay.Heading title="Add Member" onDismiss={onCloseModal} />
        <Overlay.Body>
          <AddMembersForm
            onCloseModal={onCloseModal}
            users={users}
            onSelect={this.handleSelectUserID}
            selectedUserIDs={selectedUserIDs}
            onSave={this.handleSave}
          />
        </Overlay.Body>
      </Overlay.Container>
    )
  }

  private handleSelectUserID = (selectedIDs: string[]) => {
    this.setState({selectedUserIDs: selectedIDs})
  }

  private handleSave = () => {
    const {users, addMember} = this.props
    const {selectedUserIDs} = this.state

    selectedUserIDs.forEach(id => {
      if (users[id]) {
        addMember({id: id, name: users[id].name})
      }
    })
  }
}
