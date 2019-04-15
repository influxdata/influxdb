// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Overlay} from 'src/clockface'
import AddMembersForm from 'src/members/components/AddMembersForm'
import FilterList from 'src/shared/components/Filter'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Actions
import {getUsers, addNewMember} from 'src/members/actions'

// Types
import {UsersMap} from 'src/members/reducers'
import {User} from '@influxdata/influx'
import {AppState, RemoteDataState} from 'src/types'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

interface StateProps {
  status: RemoteDataState
  users: UsersMap
}

interface DispatchProps {
  getUsers: typeof getUsers
  addMember: typeof addNewMember
}

interface State {
  selectedUserIDs: string[]
  searchTerm: string
  selectedMembers: UsersMap
}

type Props = StateProps & DispatchProps & WithRouterProps

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
    const {status} = this.props
    const {selectedUserIDs, searchTerm, selectedMembers} = this.state

    return (
      <GetResources resource={ResourceTypes.Users}>
        <Overlay visible={true}>
          <Overlay.Container maxWidth={500}>
            <Overlay.Heading
              title="Add Member"
              onDismiss={this.handleDismiss}
            />
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
                      onCloseModal={this.handleDismiss}
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

  private handleDismiss = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/members`)
  }

  private handleSave = () => {
    const {addMember} = this.props
    const {selectedMembers} = this.state

    Object.keys(selectedMembers).map(id => {
      addMember({id: id, name: selectedMembers[id].name})
    })

    this.handleDismiss()
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
)(withRouter<Props>(AddMembersOverlay))
