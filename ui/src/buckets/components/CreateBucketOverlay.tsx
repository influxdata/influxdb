// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Overlay} from '@influxdata/clockface'
import BucketOverlayForm from 'src/buckets/components/BucketOverlayForm'

// Utils
import {extractBucketMaxRetentionSeconds} from 'src/cloud/utils/limits'

// Constants
import {
  DEFAULT_SECONDS,
  READABLE_DEFAULT_SECONDS,
} from 'src/buckets/components/Retention'

// Types
import {Organization, Bucket, AppState} from 'src/types'

const DEFAULT_RULES = [
  {type: 'expire' as 'expire', everySeconds: DEFAULT_SECONDS},
]

interface StateProps {
  isRetentionLimitEnforced: boolean
}

interface OwnProps {
  org: Organization
  onCloseModal: () => void
  onCreateBucket: (bucket: Partial<Bucket>) => void
}

type Props = StateProps & OwnProps

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
        readableRetention: props.isRetentionLimitEnforced
          ? READABLE_DEFAULT_SECONDS
          : 'forever',
      },
      ruleType: props.isRetentionLimitEnforced ? 'expire' : null,
    }
  }

  public render() {
    const {onCloseModal} = this.props
    const {bucket, ruleType} = this.state

    return (
      <Overlay.Container maxWidth={400}>
        <Overlay.Header
          title="Create Bucket"
          onDismiss={this.props.onCloseModal}
        />
        <Overlay.Body>
          <BucketOverlayForm
            name={bucket.name}
            buttonText="Create"
            disableRenaming={false}
            ruleType={ruleType}
            onCloseModal={onCloseModal}
            onSubmit={this.handleSubmit}
            onChangeInput={this.handleChangeInput}
            retentionSeconds={this.retentionSeconds}
            onChangeRuleType={this.handleChangeRuleType}
            onChangeRetentionRule={this.handleChangeRetentionRule}
          />
        </Overlay.Body>
      </Overlay.Container>
    )
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
    this.handleCreateBucket()
  }

  private handleCreateBucket = (): void => {
    const {onCreateBucket, org} = this.props
    const orgID = org.id
    const organization = org.name

    const bucket = {
      ...this.state.bucket,
      orgID,
      organization,
    }

    onCreateBucket(bucket)
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const bucket = {...this.state.bucket, [key]: value}

    this.setState({bucket})
  }
}

const mstp = (state: AppState): StateProps => ({
  isRetentionLimitEnforced: !!extractBucketMaxRetentionSeconds(
    state.cloud.limits
  ),
})

export default connect<StateProps, {}, OwnProps>(mstp)(CreateBucketOverlay)
