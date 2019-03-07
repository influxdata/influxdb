// Libraries
import React, {PureComponent} from 'react'

// Components
import {Button, ButtonType} from '@influxdata/clockface'
import {Form, Grid} from 'src/clockface'
import SelectUsers from 'src/organizations/components/SelectUsers'

// Types
import {UsersMap} from 'src/organizations/components/Members'

interface Props {
  onCloseModal: () => void
  users: UsersMap
  onSelect: (selectedIDs: string[]) => void
  selectedUserIDs: string[]
  onSave: () => void
}

export default class AddMembersForm extends PureComponent<Props> {
  public render() {
    const {onCloseModal, users, onSelect, selectedUserIDs, onSave} = this.props

    return (
      <Form>
        <Grid>
          <Grid.Row>
            <Grid.Column>
              <Form.Element label="Select Users">
                <SelectUsers
                  users={users}
                  onSelect={onSelect}
                  selectedUserIDs={selectedUserIDs}
                />
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
}
