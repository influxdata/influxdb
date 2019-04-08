// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {WithRouterProps, withRouter} from 'react-router'

import _ from 'lodash'

// Components
import {Form, Input, Button} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'

// Types
import {Organization} from '@influxdata/influx'
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

// Actions
import {createOrg} from 'src/organizations/actions/orgs'

interface OwnProps {}

interface DispatchProps {
  createOrg: typeof createOrg
}

type Props = OwnProps & DispatchProps & WithRouterProps

interface State {
  org: Organization
  nameInputStatus: ComponentStatus
  errorMessage: string
}

class CreateOrgOverlay extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      org: {name: ''},
      nameInputStatus: ComponentStatus.Default,
      errorMessage: '',
    }
  }

  public render() {
    const {org, nameInputStatus, errorMessage} = this.state

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={500}>
          <Overlay.Heading
            title="Create Organization"
            onDismiss={this.closeModal}
          />
          <Form onSubmit={this.handleCreateOrg}>
            <Overlay.Body>
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
            </Overlay.Body>
            <Overlay.Footer>
              <Button text="Cancel" onClick={this.closeModal} />
              <Button
                text="Create"
                type={ButtonType.Submit}
                color={ComponentColor.Primary}
                status={this.submitButtonStatus}
                testID="create-org-submit-button"
              />
            </Overlay.Footer>
          </Form>
        </Overlay.Container>
      </Overlay>
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
    const {createOrg} = this.props

    await createOrg(org)
  }

  private closeModal = () => {
    this.props.router.goBack()
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

const mdtp = {
  createOrg,
}

export default withRouter(
  connect<{}, DispatchProps, OwnProps>(
    null,
    mdtp
  )(CreateOrgOverlay)
)
