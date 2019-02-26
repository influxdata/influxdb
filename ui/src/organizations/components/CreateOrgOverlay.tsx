// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

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
  OverlayFooter,
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
      <OverlayContainer maxWidth={500}>
        <OverlayHeading
          title="Create Organization"
          onDismiss={this.props.onCloseModal}
        />
        <Form onSubmit={this.handleCreateOrg}>
          <OverlayBody>
            <Form.Element label="Name" errorMessage={errorMessage}>
              <Input
                placeholder="Give your organization a name"
                name="name"
                autoFocus={true}
                value={org.name}
                onChange={this.handleChangeInput}
                status={nameInputStatus}
                testID="create-org-name-input"
              />
            </Form.Element>
          </OverlayBody>
          <OverlayFooter>
            <Button text="Cancel" onClick={onCloseModal} />
            <Button
              text="Create"
              type={ButtonType.Submit}
              color={ComponentColor.Primary}
              status={this.submitButtonStatus}
              testID="create-org-submit-button"
            />
          </OverlayFooter>
        </Form>
      </OverlayContainer>
    )
  }

  private get submitButtonStatus(): ComponentStatus {
    const {org} = this.state

    if (org.name) {
      return ComponentStatus.Default
    }

    return ComponentStatus.Disabled
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
        errorMessage: this.randomErrorMessage(key),
      })
    }

    this.setState({
      org,
      nameInputStatus: ComponentStatus.Valid,
      errorMessage: '',
    })
  }

  private randomErrorMessage = (key: string): string => {
    const messages = [
      `Imagine that! An organization without a ${key}`,
      `An organization needs a ${key}`,
      `You're not getting far without a ${key}`,
      `The organization formerly known as...`,
      `Pick a ${key}, any ${key}`,
      `Any ${key} will do`,
    ]

    return _.sample(messages)
  }
}
