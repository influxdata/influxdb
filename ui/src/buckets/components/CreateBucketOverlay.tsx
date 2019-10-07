// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Overlay} from '@influxdata/clockface'
import BucketOverlayForm from 'src/buckets/components/BucketOverlayForm'

// Utils
import {extractBucketMaxRetentionSeconds} from 'src/cloud/utils/limits'
import {extractBucketLimits} from 'src/cloud/utils/limits'

// Constants
import {DEFAULT_SECONDS} from 'src/buckets/components/Retention'

// Actions
import {createBucket} from 'src/buckets/actions'
import {LimitStatus} from 'src/cloud/actions/limits'

// Types
import {Organization, Bucket, AppState} from 'src/types'

const DEFAULT_RULES = [
  {type: 'expire' as 'expire', everySeconds: DEFAULT_SECONDS},
]

interface StateProps {
  org: Organization
  limitStatus: LimitStatus
  isRetentionLimitEnforced: boolean
}

interface DispatchProps {
  createBucket: typeof createBucket
}

interface OwnProps {
  onDismiss: () => void
}

type Props = StateProps & DispatchProps & OwnProps

interface State {
  bucket: Bucket
  ruleType: 'expire'
}

class CreateBucketOverlay extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      bucket: {
        name: '',
        retentionRules: props.isRetentionLimitEnforced ? DEFAULT_RULES : [],
      },
      ruleType: props.isRetentionLimitEnforced ? 'expire' : null,
    }
  }

  public render() {
    const {onDismiss} = this.props
    const {bucket, ruleType} = this.state

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={400}>
          <Overlay.Header title="Create Bucket" onDismiss={onDismiss} />
          <Overlay.Body>
            <BucketOverlayForm
              name={bucket.name}
              buttonText="Create"
              disableRenaming={false}
              ruleType={ruleType}
              onCloseModal={onDismiss}
              onSubmit={this.handleSubmit}
              disableSubmitButton={this.limitStatusExceeded}
              onChangeInput={this.handleChangeInput}
              retentionSeconds={this.retentionSeconds}
              onChangeRuleType={this.handleChangeRuleType}
              onChangeRetentionRule={this.handleChangeRetentionRule}
            />
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private get limitStatusExceeded(): boolean {
    const {limitStatus} = this.props
    return limitStatus === LimitStatus.EXCEEDED
  }

  private get retentionSeconds(): number {
    const rule = this.state.bucket.retentionRules.find(r => r.type === 'expire')

    if (!rule) {
      return 3600
    }

    return rule.everySeconds
  }

  private handleChangeRetentionRule = (everySeconds: number): void => {
    const bucket = {
      ...this.state.bucket,
      retentionRules: [{type: 'expire' as 'expire', everySeconds}],
    }

    this.setState({bucket})
  }

  private handleChangeRuleType = (ruleType: 'expire' | null) => {
    if (ruleType === 'expire') {
      this.setState({
        ruleType,
        bucket: {...this.state.bucket, retentionRules: DEFAULT_RULES},
      })
    } else {
      this.setState({
        ruleType,
        bucket: {...this.state.bucket, retentionRules: []},
      })
    }
  }

  private handleSubmit = (e: FormEvent<HTMLFormElement>): void => {
    e.preventDefault()
    const {org} = this.props
    const orgID = org.id
    const organization = org.name

    const bucket = {
      ...this.state.bucket,
      orgID,
      organization,
    }

    this.handleCreateBucket(bucket)
  }

  private handleCreateBucket = async (bucket: Bucket): Promise<void> => {
    const {onDismiss, createBucket} = this.props
    await createBucket(bucket)
    onDismiss()
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const bucket = {...this.state.bucket, [key]: value}

    this.setState({bucket})
  }
}

const mstp = ({orgs: {org}, cloud: {limits}}: AppState): StateProps => ({
  isRetentionLimitEnforced: !!extractBucketMaxRetentionSeconds(limits),
  org,
  limitStatus: extractBucketLimits(limits),
})

const mdtp = {
  createBucket,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(CreateBucketOverlay)
