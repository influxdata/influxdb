// Libraries
import React, {
  FC,
  ChangeEvent,
  FormEvent,
  useState,
  useEffect,
  useRef,
} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Popover,
  PopoverInteraction,
  PopoverPosition,
  Appearance,
  ComponentColor,
} from '@influxdata/clockface'
import BucketOverlayForm from 'src/buckets/components/BucketOverlayForm'

// Utils
import {
  extractBucketMaxRetentionSeconds,
  extractBucketLimits,
} from 'src/cloud/utils/limits'

// Constants
import {
  DEFAULT_SECONDS,
  READABLE_DEFAULT_SECONDS,
} from 'src/buckets/components/Retention'

// Actions
import {
  checkBucketLimits as checkBucketLimitsAction,
  LimitStatus,
} from 'src/cloud/actions/limits'
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
  limitStatus: LimitStatus
}

interface DispatchProps {
  createBucket: typeof createBucket
  checkBucketLimits: typeof checkBucketLimitsAction
}

interface OwnProps {}

type Props = OwnProps & StateProps & DispatchProps

const SelectorListCreateBucket: FC<Props> = ({
  org,
  createBucket,
  isRetentionLimitEnforced,
  limitStatus,
  checkBucketLimits,
}) => {
  const triggerRef = useRef<HTMLButtonElement>(null)
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

  useEffect(() => {
    // Check bucket limits when component mounts
    checkBucketLimits()
  }, [])

  const limitExceeded = limitStatus === LimitStatus.EXCEEDED

  let selectorItemClassName = 'selector-list--item'
  let titleText = 'Click to create a bucket'
  let buttonDisabled = false

  if (limitExceeded) {
    selectorItemClassName = 'selector-list--item__disabled'
    titleText = 'This account has the maximum number of buckets allowed'
    buttonDisabled = true
  }

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

  const handleSubmit = (onHide: () => void) => (
    e: FormEvent<HTMLFormElement>
  ): void => {
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
    onHide()
  }

  const handleChangeInput = (e: ChangeEvent<HTMLInputElement>): void => {
    const value = e.target.value

    if (e.target.name === 'name') {
      setBucketName(value)
    }
  }

  return (
    <>
      <button
        className={selectorItemClassName}
        data-testid="selector-list add-bucket"
        disabled={buttonDisabled}
        title={titleText}
        ref={triggerRef}
      >
        + Create Bucket
      </button>
      <Popover
        triggerRef={triggerRef}
        appearance={Appearance.Outline}
        color={ComponentColor.Primary}
        position={PopoverPosition.Above}
        showEvent={PopoverInteraction.Click}
        hideEvent={PopoverInteraction.Click}
        testID="create-bucket-popover"
        contents={onHide => (
          <BucketOverlayForm
            name={bucketName}
            buttonText="Create"
            disableRenaming={false}
            ruleType={bucketRuleType}
            onClose={onHide}
            onSubmit={handleSubmit(onHide)}
            onChangeInput={handleChangeInput}
            retentionSeconds={retentionSeconds}
            onChangeRuleType={handleChangeRuleType}
            onChangeRetentionRule={handleChangeRetentionRule}
          />
        )}
      />
    </>
  )
}

const mstp = (state: AppState): StateProps => {
  const org = getOrg(state)
  const isRetentionLimitEnforced = !!extractBucketMaxRetentionSeconds(
    state.cloud.limits
  )
  const limitStatus = extractBucketLimits(state.cloud.limits)

  return {
    org,
    isRetentionLimitEnforced,
    limitStatus,
  }
}

const mdtp: DispatchProps = {
  createBucket,
  checkBucketLimits: checkBucketLimitsAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(SelectorListCreateBucket)
