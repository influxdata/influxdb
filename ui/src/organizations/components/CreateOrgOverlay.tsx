// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {RouteComponentProps, withRouter} from 'react-router-dom'

import {sample, startCase} from 'lodash'

// Components
import {Form, Input, Button, Overlay} from '@influxdata/clockface'

// Types
import {Bucket, Organization} from 'src/types'
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

// Actions
import {createOrgWithBucket} from 'src/organizations/actions/thunks'

interface OwnProps {}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps & RouteComponentProps

interface State {
  org: Organization
  bucket: Bucket
  orgNameInputStatus: ComponentStatus
  bucketNameInputStatus: ComponentStatus
  orgErrorMessage: string
  bucketErrorMessage: string
}

class CreateOrgOverlay extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      org: {name: ''},
      bucket: {name: '', retentionRules: [], readableRetention: 'forever'},
      orgNameInputStatus: ComponentStatus.Default,
      bucketNameInputStatus: ComponentStatus.Default,
      orgErrorMessage: '',
      bucketErrorMessage: '',
    }
  }

  public render() {
    const {
      org,
      orgNameInputStatus,
      orgErrorMessage,
      bucket,
      bucketNameInputStatus,
      bucketErrorMessage,
    } = this.state

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={500}>
          <Overlay.Header
            title="Create Organization"
            onDismiss={this.closeModal}
            testID="create-org-overlay--header"
          />
          <Form onSubmit={this.handleCreateOrg}>
            <Overlay.Body>
              <Form.Element
                label="Organization Name"
                errorMessage={orgErrorMessage}
              >
                <Input
                  placeholder="Give your organization a name"
                  name="name"
                  autoFocus={true}
                  value={org.name}
                  onChange={this.handleChangeOrgInput}
                  status={orgNameInputStatus}
                  testID="create-org-name-input"
                />
              </Form.Element>
              <Form.Element
                label="Bucket Name"
                errorMessage={bucketErrorMessage}
              >
                <Input
                  placeholder="Give your bucket a name"
                  name="name"
                  autoFocus={false}
                  value={bucket.name}
                  onChange={this.handleChangeBucketInput}
                  status={bucketNameInputStatus}
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
    const {org, bucket} = this.state

    if (org.name && bucket.name) {
      return ComponentStatus.Default
    }

    return ComponentStatus.Disabled
  }

  private handleCreateOrg = async () => {
    const {org, bucket} = this.state
    const {createOrgWithBucket} = this.props

    await createOrgWithBucket(org, bucket)
  }

  private closeModal = () => {
    this.props.history.goBack()
  }

  private handleChangeOrgInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const org = {...this.state.org, [key]: value}

    if (!value) {
      return this.setState({
        org,
        orgNameInputStatus: ComponentStatus.Error,
        orgErrorMessage: this.randomErrorMessage(key, 'organization'),
      })
    }

    this.setState({
      org,
      orgNameInputStatus: ComponentStatus.Valid,
      orgErrorMessage: '',
    })
  }

  private handleChangeBucketInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const bucket = {...this.state.bucket, [key]: value}

    if (!value) {
      return this.setState({
        bucket,
        bucketNameInputStatus: ComponentStatus.Error,
        bucketErrorMessage: this.randomErrorMessage(key, 'bucket'),
      })
    }

    this.setState({
      bucket,
      bucketNameInputStatus: ComponentStatus.Valid,
      bucketErrorMessage: '',
    })
  }

  private randomErrorMessage = (key: string, resource: string): string => {
    const messages = [
      `Imagine that! ${startCase(resource)} without a ${key}`,
      `${startCase(resource)} needs a ${key}`,
      `You're not getting far without a ${key}`,
      `The ${resource} formerly known as...`,
      `Pick a ${key}, any ${key}`,
      `Any ${key} will do`,
    ]

    return sample(messages)
  }
}

const mdtp = {
  createOrgWithBucket,
}

const connector = connect(null, mdtp)

export default connector(withRouter(CreateOrgOverlay))
