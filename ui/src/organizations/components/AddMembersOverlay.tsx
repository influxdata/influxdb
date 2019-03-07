// Libraries
import React, {PureComponent} from 'react'

// Components
import {OverlayBody, OverlayHeading, OverlayContainer} from 'src/clockface'
import AddMembersForm from './AddMembersForm'
import {AddResourceMemberRequestBody} from '@influxdata/influx'

// Types
import {UsersMap} from 'src/organizations/components/Members'

interface Props {
  onCloseModal: () => void
  users: UsersMap
  addUser: (user: AddResourceMemberRequestBody) => void
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
      <OverlayContainer maxWidth={500}>
        <OverlayHeading title="Add Member" onDismiss={onCloseModal} />
        <OverlayBody>
          <AddMembersForm
            onCloseModal={onCloseModal}
            users={users}
            onSelect={this.handleSelectUserID}
            selectedUserIDs={selectedUserIDs}
            onSave={this.handleSave}
          />
        </OverlayBody>
      </OverlayContainer>
    )
  }

  private handleSelectUserID = (selectedIDs: string[]) => {
    this.setState({selectedUserIDs: selectedIDs})
  }

  private handleSave = () => {
    const {users, addUser} = this.props
    const {selectedUserIDs} = this.state

    selectedUserIDs.forEach(id => {
      if (users[id]) {
        addUser({id: id, name: users[id].name})
      }
    })
  }
}
