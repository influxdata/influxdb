// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Button, ButtonType, IconFont} from '@influxdata/clockface'
import {Form, Grid, Input, Columns} from 'src/clockface'
import SelectUsers from 'src/organizations/components/SelectUsers'
import {UsersMap} from 'src/organizations/components/Members'

// Types
import {User} from '@influxdata/influx'

interface Props {
  onCloseModal: () => void
  users: User[]
  onSelect: (selectedIDs: string[]) => void
  selectedUserIDs: string[]
  onSave: () => void
  searchTerm: string
  onFilterChange: (e: ChangeEvent<HTMLInputElement>) => void
  onFilterBlur: (e: ChangeEvent<HTMLInputElement>) => void
  selectedMembers: UsersMap
}

export default class AddMembersForm extends PureComponent<Props> {
  public render() {
    const {
      onCloseModal,
      users,
      onSelect,
      selectedUserIDs,
      onSave,
      searchTerm,
      onFilterChange,
      onFilterBlur,
    } = this.props

    return (
      <Form>
        <Grid>
          <Grid.Row>
            <Grid.Column widthSM={Columns.Six}>
              <Form.Element label="Filter Users">
                <Input
                  icon={IconFont.Search}
                  placeholder="Filter users..."
                  widthPixels={200}
                  value={searchTerm}
                  onChange={onFilterChange}
                  onBlur={onFilterBlur}
                />
              </Form.Element>
              <Form.Element label="Select Users">
                <SelectUsers
                  users={users}
                  onSelect={onSelect}
                  selectedUserIDs={selectedUserIDs}
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthSM={Columns.Six}>
              <Form.Element label="Members to be added">
                <ul>{this.membersSelected}</ul>
              </Form.Element>
            </Grid.Column>
          </Grid.Row>
          <Grid.Row>
            <Grid.Column>
              <Form.Footer>
                <Button
                  text="Cancel"
                  type={ButtonType.Button}
                  onClick={onCloseModal}
                />
                <Button
                  text="Save Changes"
                  type={ButtonType.Submit}
                  onClick={onSave}
                />
              </Form.Footer>
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Form>
    )
  }

  private get membersSelected(): JSX.Element[] {
    const {selectedMembers} = this.props

    if (selectedMembers) {
      return Object.keys(selectedMembers).map(id => (
        <li key={id}>{selectedMembers[id].name}</li>
      ))
    }
    return
  }
}
