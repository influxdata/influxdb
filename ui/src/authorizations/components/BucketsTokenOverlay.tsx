import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Form} from 'src/clockface'
import {
  IconFont,
  ComponentColor,
  ComponentSpacer,
  AlignItems,
  FlexDirection,
  ComponentSize,
  Button,
  ButtonType,
  Grid,
  Columns,
  Input,
  Overlay,
} from '@influxdata/clockface'
import BucketsSelector from 'src/authorizations/components/BucketsSelector'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

// Utils
import {
  specificBucketsPermissions,
  selectBucket,
} from 'src/authorizations/utils/permissions'

// Actions
import {createAuthorization} from 'src/authorizations/actions'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {AppState} from 'src/types'
import {Bucket, Permission, Authorization} from '@influxdata/influx'

interface StateProps {
  buckets: Bucket[]
}

interface DispatchProps {
  onCreateAuthorization: typeof createAuthorization
}

interface State {
  description: string
  readBuckets: string[]
  writeBuckets: string[]
}

type Props = WithRouterProps & DispatchProps & StateProps

@ErrorHandling
class BucketsTokenOverlay extends PureComponent<Props, State> {
  public state = {description: '', readBuckets: [], writeBuckets: []}

  render() {
    const {buckets} = this.props
    const {description, readBuckets, writeBuckets} = this.state

    return (
      <Overlay visible={true}>
        <Overlay.Container>
          <Overlay.Header
            title="Generate Read/Write Token"
            onDismiss={this.handleDismiss}
          />
          <Overlay.Body>
            <Form onSubmit={this.handleSave}>
              <ComponentSpacer
                alignItems={AlignItems.Center}
                direction={FlexDirection.Column}
                margin={ComponentSize.Large}
              >
                <Form.Element label="Description">
                  <Input
                    placeholder="Describe this new token"
                    value={description}
                    onChange={this.handleInputChange}
                  />
                </Form.Element>
                <Form.Element label="">
                  <GetResources resource={ResourceTypes.Buckets}>
                    <Grid.Row>
                      <Grid.Column
                        widthXS={Columns.Twelve}
                        widthSM={Columns.Six}
                      >
                        <BucketsSelector
                          onSelect={this.handleSelectReadBucket}
                          buckets={buckets}
                          selectedBuckets={readBuckets}
                          title="Read"
                          onSelectAll={this.handleReadSelectAllBuckets}
                          onDeselectAll={this.handleReadDeselectAllBuckets}
                        />
                      </Grid.Column>
                      <Grid.Column
                        widthXS={Columns.Twelve}
                        widthSM={Columns.Six}
                      >
                        <BucketsSelector
                          onSelect={this.handleSelectWriteBucket}
                          buckets={buckets}
                          selectedBuckets={writeBuckets}
                          title="Write"
                          onSelectAll={this.handleWriteSelectAllBuckets}
                          onDeselectAll={this.handleWriteDeselectAllBuckets}
                        />
                      </Grid.Column>
                    </Grid.Row>
                  </GetResources>
                </Form.Element>
                <ComponentSpacer
                  alignItems={AlignItems.Center}
                  direction={FlexDirection.Row}
                  margin={ComponentSize.Small}
                >
                  <Button
                    text="Cancel"
                    icon={IconFont.Remove}
                    onClick={this.handleDismiss}
                  />

                  <Button
                    text="Save"
                    icon={IconFont.Checkmark}
                    color={ComponentColor.Success}
                    type={ButtonType.Submit}
                  />
                </ComponentSpacer>
              </ComponentSpacer>
            </Form>
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private handleSelectReadBucket = (bucketName: string): void => {
    const readBuckets = selectBucket(bucketName, this.state.readBuckets)

    this.setState({readBuckets})
  }

  private handleSelectWriteBucket = (bucketName: string): void => {
    const writeBuckets = selectBucket(bucketName, this.state.writeBuckets)

    this.setState({writeBuckets})
  }

  private handleReadSelectAllBuckets = () => {
    const readBuckets = this.props.buckets.map(b => b.name)
    this.setState({readBuckets})
  }

  private handleReadDeselectAllBuckets = () => {
    this.setState({readBuckets: []})
  }
  j
  private handleWriteSelectAllBuckets = () => {
    const writeBuckets = this.props.buckets.map(b => b.name)
    this.setState({writeBuckets})
  }

  private handleWriteDeselectAllBuckets = () => {
    this.setState({writeBuckets: []})
  }

  private handleSave = async () => {
    const {
      params: {orgID},
      onCreateAuthorization,
    } = this.props

    const permissions = [
      ...this.writeBucketPermissions,
      ...this.readBucketPermissions,
    ]

    const token: Authorization = {
      orgID,
      description: this.state.description,
      permissions,
    }

    await onCreateAuthorization(token)

    this.handleDismiss()
  }

  private get writeBucketPermissions(): Permission[] {
    const {buckets} = this.props

    const writeBuckets = this.state.writeBuckets.map(bucketName => {
      return buckets.find(b => b.name === bucketName)
    })

    return specificBucketsPermissions(writeBuckets, Permission.ActionEnum.Write)
  }

  private get readBucketPermissions(): Permission[] {
    const {buckets} = this.props

    const readBuckets = this.state.readBuckets.map(bucketName => {
      return buckets.find(b => b.name === bucketName)
    })

    return specificBucketsPermissions(readBuckets, Permission.ActionEnum.Read)
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target

    this.setState({description: value})
  }

  private handleDismiss = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/tokens`)
  }
}

const mstp = ({buckets: {list}}: AppState): StateProps => {
  return {buckets: list}
}

const mdtp: DispatchProps = {
  onCreateAuthorization: createAuthorization,
}

export default connect<{}, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(BucketsTokenOverlay))
