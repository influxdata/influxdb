// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Form,
  OverlayBody,
  OverlayHeading,
  OverlayContainer,
  Input,
  Button,
  ComponentColor,
  ComponentStatus,
} from 'src/clockface'

// Types
import {Organization} from 'src/types/v2'
import {createOrg} from 'src/organizations/actions'

interface Props {
  link: string
  onCreateOrg: typeof createOrg
  onCloseModal: () => void
}

interface State {
  org: Partial<Organization>
  nameInputStatus: ComponentStatus
  errorMessage: string
}

export default class CreateOrgOverlay extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      org: {},
      nameInputStatus: ComponentStatus.Default,
      errorMessage: '',
    }
  }

  public render() {
    const {onCloseModal} = this.props
    const {org, nameInputStatus, errorMessage} = this.state

    return (
      <OverlayContainer>
        <OverlayHeading
          title="Create Organization"
          onDismiss={this.props.onCloseModal}
        />
        <OverlayBody>
          <Form>
            <Form.Element label="Name" errorMessage={errorMessage}>
              <Input
                placeholder="Give your organization a name"
                name="name"
                autoFocus={true}
                value={org.name}
                onChange={this.handleChangeInput}
                status={nameInputStatus}
              />
            </Form.Element>
            <Form.Footer>
              <Button
                text="Cancel"
                color={ComponentColor.Danger}
                onClick={onCloseModal}
              />
              <Button
                text="Create"
                color={ComponentColor.Primary}
                onClick={this.handleCreateOrg}
              />
            </Form.Footer>
          </Form>
        </OverlayBody>
      </OverlayContainer>
    )
  }

  private handleCreateOrg = async () => {
    const {org} = this.state
    const {link, onCreateOrg, onCloseModal} = this.props
    await onCreateOrg(link, org)
    onCloseModal()
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const org = {...this.state.org, [key]: value}

    if (!value) {
      return this.setState({
        org,
        nameInputStatus: ComponentStatus.Error,
        errorMessage: `Organization ${key} cannot be empty`,
      })
    }

    this.setState({
      org,
      nameInputStatus: ComponentStatus.Valid,
      errorMessage: '',
    })
  }
}
