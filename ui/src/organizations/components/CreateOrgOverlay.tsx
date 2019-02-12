// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Button,
  ComponentColor,
  ComponentStatus,
  ButtonType,
} from '@influxdata/clockface'
import {
  Form,
  OverlayBody,
  OverlayHeading,
  OverlayContainer,
  Input,
} from 'src/clockface'

// Types
import {Organization} from '@influxdata/influx'
import {createOrg} from 'src/organizations/actions'

interface Props {
  link: string
  onCreateOrg: typeof createOrg
  onCloseModal: () => void
}

interface State {
  org: Organization
  nameInputStatus: ComponentStatus
  errorMessage: string
}

export default class CreateOrgOverlay extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      org: {name: ''},
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
          <Form onSubmit={this.handleCreateOrg}>
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
                type={ButtonType.Submit}
                color={ComponentColor.Primary}
              />
            </Form.Footer>
          </Form>
        </OverlayBody>
      </OverlayContainer>
    )
  }

  private handleCreateOrg = async () => {
    const {org} = this.state
    const {onCreateOrg, onCloseModal} = this.props
    await onCreateOrg(org)
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
