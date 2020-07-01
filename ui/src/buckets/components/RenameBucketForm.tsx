// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import {ComponentStatus} from 'src/clockface'
import {
  Form,
  Input,
  Button,
  ButtonType,
  ComponentColor,
  FlexBox,
  AlignItems,
  FlexDirection,
  ComponentSize,
} from '@influxdata/clockface'

// Actions
import {renameBucket} from 'src/buckets/actions/thunks'

// Types
import {AppState, Bucket, ResourceType, OwnBucket} from 'src/types'

// Selectors
import {getAll, getByID} from 'src/resources/selectors'

interface State {
  bucket: OwnBucket
}

interface StateProps {
  startBucket: OwnBucket
  buckets: Bucket[]
}

interface DispatchProps {
  onRenameBucket: typeof renameBucket
}

type Props = StateProps &
  DispatchProps &
  RouteComponentProps<{bucketID: string; orgID: string}>

class RenameBucketForm extends PureComponent<Props, State> {
  public state = {bucket: this.props.startBucket}

  public render() {
    const {bucket} = this.state

    return (
      <Form onSubmit={this.handleSubmit}>
        <Form.ValidationElement
          label="Name"
          validationFunc={this.handleValidation}
          value={bucket.name}
        >
          {status => (
            <>
              <FlexBox
                alignItems={AlignItems.Center}
                direction={FlexDirection.Column}
                margin={ComponentSize.Large}
              >
                <Input
                  placeholder="Give your bucket a name"
                  name="name"
                  autoFocus={true}
                  value={bucket.name}
                  onChange={this.handleChangeInput}
                  status={status}
                />
                <FlexBox
                  alignItems={AlignItems.Center}
                  direction={FlexDirection.Row}
                  margin={ComponentSize.Small}
                >
                  <Button
                    text="Cancel"
                    onClick={this.handleClose}
                    type={ButtonType.Button}
                  />
                  <Button
                    text="Change Bucket Name"
                    color={ComponentColor.Danger}
                    status={this.saveButtonStatus(status)}
                    type={ButtonType.Submit}
                  />
                </FlexBox>
              </FlexBox>
            </>
          )}
        </Form.ValidationElement>
      </Form>
    )
  }

  private saveButtonStatus = (
    validationStatus: ComponentStatus
  ): ComponentStatus => {
    if (
      this.state.bucket.name === this.props.startBucket.name ||
      validationStatus === ComponentStatus.Error
    ) {
      return ComponentStatus.Disabled
    }

    return validationStatus
  }

  private handleValidation = (bucketName: string): string | null => {
    if (!bucketName) {
      return 'Name is required'
    }

    if (this.props.buckets.find(b => b.name === bucketName)) {
      return 'This bucket name is taken'
    }
  }

  private handleSubmit = (): void => {
    const {startBucket, onRenameBucket} = this.props
    const {bucket} = this.state

    onRenameBucket(startBucket.name, bucket)
    this.handleClose()
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const name = e.target.value
    const bucket = {...this.state.bucket, name}

    this.setState({bucket})
  }

  private handleClose = () => {
    const {history, match} = this.props

    history.push(`/orgs/${match.params.orgID}/load-data/buckets`)
  }
}

const mstp = (state: AppState, props: Props): StateProps => {
  const {bucketID} = props.match.params

  const startBucket = getByID<OwnBucket>(state, ResourceType.Buckets, bucketID)
  const buckets = getAll<Bucket>(state, ResourceType.Buckets).filter(
    b => b.id !== bucketID
  )

  return {
    startBucket,
    buckets,
  }
}

const mdtp: DispatchProps = {
  onRenameBucket: renameBucket,
}

// state mapping requires router
export default withRouter(
  connect<StateProps, DispatchProps>(mstp, mdtp)(RenameBucketForm)
)
