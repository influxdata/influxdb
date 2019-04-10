// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Overlay} from 'src/clockface'
import AddMembersForm from 'src/members/components/AddMembersForm'
import FilterList from 'src/shared/components/Filter'

// Types
import {UsersMap} from 'src/members/components/Members'
import {User} from '@influxdata/influx'
import {Member} from 'src/types'

interface Props {
  onCloseModal: () => void
  users: UsersMap
  addMember: (member: Member) => void
}

interface State {
  selectedUserIDs: string[]
  searchTerm: string
  selectedMembers: UsersMap
}

export default class AddMembersOverlay extends PureComponent<Props, State> {
  public state: State = {
    searchTerm: '',
    selectedUserIDs: [],
    selectedMembers: {},
  }

  constructor(props) {
    super(props)
  }

  public render() {
    const {onCloseModal} = this.props
    const {selectedUserIDs, searchTerm, selectedMembers} = this.state

    return (
      <Overlay.Container maxWidth={500}>
        <Overlay.Heading title="Add Member" onDismiss={onCloseModal} />
        <Overlay.Body>
          <FilterList<User>
            list={this.filteredList}
            searchTerm={searchTerm}
            searchKeys={['name']}
          >
            {ts => (
              <AddMembersForm
                onCloseModal={onCloseModal}
                users={ts}
                onSelect={this.handleSelectUserID}
                selectedUserIDs={selectedUserIDs}
                onSave={this.handleSave}
                searchTerm={searchTerm}
                onFilterChange={this.handleFilterChange}
                onFilterBlur={this.handleFilterBlur}
                selectedMembers={selectedMembers}
              />
            )}
          </FilterList>
        </Overlay.Body>
      </Overlay.Container>
    )
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.handleFilterUpdate(e.target.value)
  }

  private handleFilterUpdate = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private get filteredList(): User[] {
    const {users} = this.props
    const userValues = Object.values(users)
    return userValues
  }

  private handleSelectUserID = (selectedIDs: string[]) => {
    const {users} = this.props
    const membersSelected = {}

    selectedIDs.forEach(key => {
      membersSelected[key] = users[key]
    })

    this.setState({
      selectedUserIDs: selectedIDs,
      selectedMembers: membersSelected,
    })
  }

  private handleSave = () => {
    const {addMember, onCloseModal} = this.props
    const {selectedMembers} = this.state

    Object.keys(selectedMembers).map(id => {
      addMember({id: id, name: selectedMembers[id].name})
    })

    onCloseModal()
  }
}
