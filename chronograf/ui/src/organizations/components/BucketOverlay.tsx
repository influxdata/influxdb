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
import RetentionPeriod from 'src/organizations/components/RetentionPeriod'

// Types
import {Bucket} from 'src/types/v2'

interface Props {
  link: string
  onCreateBucket: (link: string, bucket: Partial<Bucket>) => Promise<void>
  onCloseModal: () => void
}

interface State {
  bucket: Partial<Bucket>
  nameInputStatus: ComponentStatus
  errorMessage: string
}

export default class CreateOrgOverlay extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      bucket: {retentionPeriod: 0},
      nameInputStatus: ComponentStatus.Default,
      errorMessage: '',
    }
  }

  public render() {
    const {onCloseModal} = this.props
    const {bucket, nameInputStatus, errorMessage} = this.state

    return (
      <OverlayContainer>
        <OverlayHeading
          title="Create Bucket"
          onDismiss={this.props.onCloseModal}
        />
        <OverlayBody>
          <Form>
            <Form.Element label="Name" errorMessage={errorMessage}>
              <Input
                placeholder="Give your bucket a name"
                name="name"
                autoFocus={true}
                value={bucket.name}
                onChange={this.handleChangeInput}
                status={nameInputStatus}
              />
            </Form.Element>
            <RetentionPeriod
              retentionPeriod={bucket.retentionPeriod}
              onChangeRetentionPeriod={this.handleChangeRetentionPeriod}
            />
            <Form.Footer>
              <Button
                text="Cancel"
                color={ComponentColor.Danger}
                onClick={onCloseModal}
              />
              <Button
                text="Create"
                color={ComponentColor.Primary}
                onClick={this.handleCreateBucket}
              />
            </Form.Footer>
          </Form>
        </OverlayBody>
      </OverlayContainer>
    )
  }

  private handleChangeRetentionPeriod = (retentionPeriod: number): void => {
    const {bucket} = this.state
    this.setState({bucket: {...bucket, retentionPeriod}})
  }

  private handleCreateBucket = () => {
    const {bucket} = this.state
    const {link, onCreateBucket} = this.props
    onCreateBucket(link, bucket)
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const bucket = {...this.state.bucket, [key]: value}

    if (!value) {
      return this.setState({
        bucket,
        nameInputStatus: ComponentStatus.Error,
        errorMessage: `Bucket ${key} cannot be empty`,
      })
    }

    this.setState({
      bucket,
      nameInputStatus: ComponentStatus.Valid,
      errorMessage: '',
    })
  }
}
