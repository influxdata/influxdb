// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Form,
  Input,
  Button,
  Grid,
  ComponentSpacer,
  Icon,
} from '@influxdata/clockface'
import SelectUsers from 'src/members/components/SelectUsers'
import {UsersMap} from 'src/members/reducers'

// Types
import {User} from '@influxdata/influx'
import {
  Columns,
  IconFont,
  ButtonType,
  FlexDirection,
  AlignItems,
} from '@influxdata/clockface'
import {ComponentSize} from 'src/clockface'

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
                <ComponentSpacer
                  direction={FlexDirection.Column}
                  alignItems={AlignItems.Stretch}
                  margin={ComponentSize.ExtraSmall}
                >
                  {this.membersSelected}
                </ComponentSpacer>
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
        <Form.Box key={id} className="selected-member">
          <Icon glyph={IconFont.User} />
          <p>{selectedMembers[id].name}</p>
        </Form.Box>
      ))
    }
    return
  }
}
