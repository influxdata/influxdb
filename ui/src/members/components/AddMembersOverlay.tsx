// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import AddMembersForm from 'src/members/components/AddMembersForm'
import FilterList from 'src/shared/components/Filter'
import {SpinnerContainer, TechnoSpinner, Overlay} from '@influxdata/clockface'

// Actions
import {getUsers, addNewMember} from 'src/members/actions'

// Types
import {UsersMap} from 'src/members/reducers'
import {User} from '@influxdata/influx'
import {AppState, RemoteDataState} from 'src/types'
import GetResources, {ResourceType} from 'src/shared/components/GetResources'

interface StateProps {
  status: RemoteDataState
  users: UsersMap
}

interface DispatchProps {
  getUsers: typeof getUsers
  addMember: typeof addNewMember
}

interface OwnProps {
  onDismiss: () => void
}

interface State {
  selectedUserIDs: string[]
  searchTerm: string
  selectedMembers: UsersMap
}

type Props = OwnProps & StateProps & DispatchProps

class AddMembersOverlay extends PureComponent<Props, State> {
  public state: State = {
    searchTerm: '',
    selectedUserIDs: [],
    selectedMembers: {},
  }

  constructor(props) {
    super(props)
  }

  public render() {
    const {status, onDismiss} = this.props
    const {selectedUserIDs, searchTerm, selectedMembers} = this.state

    return (
      <GetResources resource={ResourceType.Users}>
        <Overlay visible={true}>
          <Overlay.Container maxWidth={720}>
            <Overlay.Header title="Add Member" onDismiss={onDismiss} />
            <Overlay.Body>
              <SpinnerContainer
                loading={status}
                spinnerComponent={<TechnoSpinner />}
              >
                <FilterList<User>
                  list={this.filteredList}
                  searchTerm={searchTerm}
                  searchKeys={['name']}
                >
                  {ts => (
                    <AddMembersForm
                      onCloseModal={onDismiss}
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
              </SpinnerContainer>
            </Overlay.Body>
          </Overlay.Container>
        </Overlay>
      </GetResources>
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
    const {addMember, onDismiss} = this.props
    const {selectedMembers} = this.state

    Object.keys(selectedMembers).map(id => {
      addMember({id: id, name: selectedMembers[id].name})
    })

    onDismiss()
  }
}

const mstp = ({members: {users}}: AppState): StateProps => {
  return {status: users.status, users: users.item}
}

const mdtp: DispatchProps = {
  getUsers: getUsers,
  addMember: addNewMember,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(AddMembersOverlay)
