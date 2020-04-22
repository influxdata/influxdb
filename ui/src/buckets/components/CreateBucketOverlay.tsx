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

// Actions
import {
  createBucket,
} from 'src/buckets/actions/thunks'

// Types
import {Organization, Bucket, AppState} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'

const DEFAULT_RULES = [
  {type: 'expire' as 'expire', everySeconds: DEFAULT_SECONDS},
]

interface StateProps {
  org: Organization
  isRetentionLimitEnforced: boolean
}

interface DispatchProps {
  createBucket: typeof createBucket
}

interface OwnProps {
  onClose: () => void
  createBucket: (bucket: Partial<Bucket>) => void
}

type Props = StateProps & OwnProps & DispatchProps

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
    const {onClose} = this.props
    const {bucket, ruleType} = this.state

    return (
      <Overlay.Container maxWidth={400}>
        <Overlay.Header
          title="Create Bucket"
          onDismiss={onClose}
        />
        <Overlay.Body>
          <BucketOverlayForm
            name={bucket.name}
            buttonText="Create"
            disableRenaming={false}
            ruleType={ruleType}
            onClose={onClose}
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
    const {createBucket, org, onClose} = this.props
    const orgID = org.id
    const organization = org.name

    const bucket: Partial<Bucket> = {
      ...this.state.bucket,
      orgID,
      organization,
    }

    createBucket(bucket)
    onClose()
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const bucket = {...this.state.bucket, [key]: value}

    this.setState({bucket})
  }
}

const mstp = (state: AppState): StateProps => {
  const org = getOrg(state)
  const isRetentionLimitEnforced = !!extractBucketMaxRetentionSeconds(
    state.cloud.limits
  )

    return {
    org,
    isRetentionLimitEnforced,
  }
}

const mdtp: DispatchProps = {
  createBucket,
}

export default connect<StateProps, DispatchProps, OwnProps>(mstp, mdtp)(CreateBucketOverlay)
