// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Form, Input, ComponentColor, Button} from 'src/clockface'

// Types
import {Organization} from 'src/types/v2'
import {updateOrg} from 'src/organizations/actions'

interface Props {
  org: Organization
  onUpdateOrg: typeof updateOrg
}

interface State {
  org: Organization
  errorMessage: string
}

export default class OrgOptions extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      org: this.props.org,
      errorMessage: '',
    }
  }

  public render() {
    const {org, errorMessage} = this.state
    return (
      <Form>
        <Form.Element label="Name" errorMessage={errorMessage}>
          <Input
            placeholder="Give your organization a name"
            name="name"
            autoFocus={true}
            value={org.name}
            onChange={this.handleChangeInput}
          />
        </Form.Element>
        <Form.Footer>
          <Button
            text="Update"
            color={ComponentColor.Primary}
            onClick={this.handleUpdateOrg}
          />
        </Form.Footer>
      </Form>
    )
  }

  private handleUpdateOrg = async (): Promise<void> => {
    await this.props.onUpdateOrg(this.state.org)
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const org = {...this.state.org, [key]: value}

    if (!value) {
      return this.setState({
        org,
        errorMessage: `Organization ${key} cannot be empty`,
      })
    }

    this.setState({
      org,
      errorMessage: '',
    })
  }
}
