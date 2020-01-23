import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  IconFont,
  ComponentColor,
  FlexBox,
  AlignItems,
  FlexDirection,
  ComponentSize,
  Button,
  ButtonType,
  Grid,
  Columns,
  Input,
  Overlay,
  Form,
} from '@influxdata/clockface'
import BucketsSelector from 'src/authorizations/components/BucketsSelector'
import GetResources from 'src/resources/components/GetResources'

// Utils
import {
  specificBucketsPermissions,
  selectBucket,
  allBucketsPermissions,
  BucketTab,
} from 'src/authorizations/utils/permissions'
import {isSystemBucket} from 'src/buckets/constants/index'
import {getOrg} from 'src/organizations/selectors'
import {getAll} from 'src/resources/selectors'

// Actions
import {createAuthorization} from 'src/authorizations/actions/thunks'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {
  AppState,
  Bucket,
  Permission,
  Authorization,
  ResourceType,
} from 'src/types'

interface OwnProps {
  onClose: () => void
}

interface StateProps {
  buckets: Bucket[]
  orgID: string
}

interface DispatchProps {
  onCreateAuthorization: typeof createAuthorization
}

interface State {
  description: string
  readBuckets: string[]
  writeBuckets: string[]
  activeTabRead: BucketTab
  activeTabWrite: BucketTab
}

type Props = OwnProps & DispatchProps & StateProps

@ErrorHandling
class BucketsTokenOverlay extends PureComponent<Props, State> {
  public state = {
    description: '',
    readBuckets: [],
    writeBuckets: [],
    activeTabRead: BucketTab.Scoped,
    activeTabWrite: BucketTab.Scoped,
  }

  render() {
    const {
      description,
      readBuckets,
      writeBuckets,
      activeTabRead,
      activeTabWrite,
    } = this.state

    return (
      <Overlay.Container maxWidth={700}>
        <Overlay.Header
          title="Generate Read/Write Token"
          onDismiss={this.handleDismiss}
        />
        <Overlay.Body>
          <Form onSubmit={this.handleSave}>
            <FlexBox
              alignItems={AlignItems.Center}
              direction={FlexDirection.Column}
              margin={ComponentSize.Large}
            >
              <Form.Element label="Description">
                <Input
                  placeholder="Describe this new token"
                  value={description}
                  onChange={this.handleInputChange}
                  testID="input-field--descr"
                />
              </Form.Element>
              <Form.Element label="">
                <GetResources resources={[ResourceType.Buckets]}>
                  <Grid.Row>
                    <Grid.Column widthXS={Columns.Twelve} widthSM={Columns.Six}>
                      <BucketsSelector
                        onSelect={this.handleSelectReadBucket}
                        buckets={this.nonSystemBuckets}
                        selectedBuckets={readBuckets}
                        title="Read"
                        onSelectAll={this.handleReadSelectAllBuckets}
                        onDeselectAll={this.handleReadDeselectAllBuckets}
                        activeTab={activeTabRead}
                        onTabClick={this.handleReadTabClick}
                      />
                    </Grid.Column>
                    <Grid.Column widthXS={Columns.Twelve} widthSM={Columns.Six}>
                      <BucketsSelector
                        onSelect={this.handleSelectWriteBucket}
                        buckets={this.nonSystemBuckets}
                        selectedBuckets={writeBuckets}
                        title="Write"
                        onSelectAll={this.handleWriteSelectAllBuckets}
                        onDeselectAll={this.handleWriteDeselectAllBuckets}
                        activeTab={activeTabWrite}
                        onTabClick={this.handleWriteTabClick}
                      />
                    </Grid.Column>
                  </Grid.Row>
                </GetResources>
              </Form.Element>
              <Form.Footer>
                <Button
                  text="Cancel"
                  icon={IconFont.Remove}
                  onClick={this.handleDismiss}
                  testID="button--cancel"
                />

                <Button
                  text="Save"
                  icon={IconFont.Checkmark}
                  color={ComponentColor.Success}
                  type={ButtonType.Submit}
                  testID="button--save"
                />
              </Form.Footer>
            </FlexBox>
          </Form>
        </Overlay.Body>
      </Overlay.Container>
    )
  }

  private handleReadTabClick = (tab: BucketTab) => {
    this.setState({activeTabRead: tab})
  }

  private handleWriteTabClick = (tab: BucketTab) => {
    this.setState({activeTabWrite: tab})
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

  private handleSave = () => {
    const {orgID, onCreateAuthorization} = this.props
    const {activeTabRead, activeTabWrite} = this.state

    let permissions = []

    if (activeTabRead === BucketTab.Scoped) {
      permissions = [...this.readBucketPermissions]
    } else {
      permissions = [...this.allReadBucketPermissions]
    }

    if (activeTabWrite === BucketTab.Scoped) {
      permissions = [...permissions, ...this.writeBucketPermissions]
    } else {
      permissions = [...permissions, ...this.allWriteBucketPermissions]
    }

    const token: Authorization = {
      orgID,
      description: this.state.description,
      permissions,
    }

    onCreateAuthorization(token)

    this.handleDismiss()
  }

  private get writeBucketPermissions(): Permission[] {
    const {buckets} = this.props

    const writeBuckets = this.state.writeBuckets.map(bucketName => {
      return buckets.find(b => b.name === bucketName)
    })

    return specificBucketsPermissions(writeBuckets, 'write')
  }

  private get readBucketPermissions(): Permission[] {
    const {buckets} = this.props

    const readBuckets = this.state.readBuckets.map(bucketName => {
      return buckets.find(b => b.name === bucketName)
    })

    return specificBucketsPermissions(readBuckets, 'read')
  }

  private get allReadBucketPermissions(): Permission[] {
    const {orgID} = this.props

    return allBucketsPermissions(orgID, 'read')
  }

  private get allWriteBucketPermissions(): Permission[] {
    const {orgID} = this.props

    return allBucketsPermissions(orgID, 'write')
  }

  private get nonSystemBuckets(): Bucket[] {
    const {buckets} = this.props

    return buckets.filter(bucket => !isSystemBucket(bucket.name))
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target

    this.setState({description: value})
  }

  private handleDismiss = () => {
    this.props.onClose()
  }
}

const mstp = (state: AppState): StateProps => {
  return {
    orgID: getOrg(state).id,
    buckets: getAll<Bucket>(state, ResourceType.Buckets),
  }
}

const mdtp: DispatchProps = {
  onCreateAuthorization: createAuthorization,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(BucketsTokenOverlay)
