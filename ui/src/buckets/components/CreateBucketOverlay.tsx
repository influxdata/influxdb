// Libraries
import React, {FC, ChangeEvent, FormEvent, useState} from 'react'
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
import {createBucket} from 'src/buckets/actions/thunks'

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
}

type Props = OwnProps & StateProps & DispatchProps

const CreateBucketOverlay: FC<Props> = ({
  org,
  isRetentionLimitEnforced,
  createBucket,
  onClose,
}) => {
  const [bucketName, setBucketName] = useState<string>('')
  const [bucketRetentionRules, setBucketRetentionRules] = useState<any>(
    isRetentionLimitEnforced ? DEFAULT_RULES : []
  )
  const [bucketRuleType, setBucketRuleType] = useState<'expire' | null>(
    'expire'
  )
  const bucketReadableRetention = isRetentionLimitEnforced
    ? READABLE_DEFAULT_SECONDS
    : 'forever'

  const retentionRule = bucketRetentionRules.find(r => r.type === 'expire')
  const retentionSeconds = retentionRule ? retentionRule.everySeconds : 3600

  const handleChangeRuleType = (ruleType: 'expire' | null): void => {
    if (ruleType === 'expire') {
      setBucketRetentionRules(DEFAULT_RULES)
    } else {
      setBucketRetentionRules([])
    }
    setBucketRuleType(ruleType)
  }

  const handleChangeRetentionRule = (everySeconds: number): void => {
    setBucketRetentionRules([{type: 'expire' as 'expire', everySeconds}])
  }

  const handleSubmit = (e: FormEvent<HTMLFormElement>): void => {
    e.preventDefault()

    const orgID = org.id
    const bucket: Bucket = {
      name: bucketName,
      type: 'user',
      retentionRules: bucketRetentionRules,
      readableRetention: bucketReadableRetention,
      orgID,
    }

    createBucket(bucket)
    onClose()
  }

  const handleChangeInput = (e: ChangeEvent<HTMLInputElement>): void => {
    const value = e.target.value

    if (e.target.name === 'name') {
      setBucketName(value)
    }
  }

  return (
    <Overlay.Container maxWidth={400}>
      <Overlay.Header title="Create Bucket" onDismiss={onClose} />
      <Overlay.Body>
        <BucketOverlayForm
          name={bucketName}
          buttonText="Create"
          disableRenaming={false}
          ruleType={bucketRuleType}
          onClose={onClose}
          onSubmit={handleSubmit}
          onChangeInput={handleChangeInput}
          retentionSeconds={retentionSeconds}
          onChangeRuleType={handleChangeRuleType}
          onChangeRetentionRule={handleChangeRetentionRule}
        />
      </Overlay.Body>
    </Overlay.Container>
  )
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

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(CreateBucketOverlay)
