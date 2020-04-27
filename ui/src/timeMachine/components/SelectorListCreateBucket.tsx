// Libraries
import React, {
  FC,
  ChangeEvent,
  FormEvent,
  useEffect,
  useRef,
  useReducer,
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

// Actions
import {
  checkBucketLimits as checkBucketLimitsAction,
  LimitStatus,
} from 'src/cloud/actions/limits'
import {createBucket} from 'src/buckets/actions/thunks'

// Types
import {Organization, AppState} from 'src/types'
import {
  createBucketReducer,
  RuleType,
  initialBucketState,
  DEFAULT_RULES,
} from 'src/buckets/reducers/createBucket'

// Selectors
import {getOrg} from 'src/organizations/selectors'

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
  const [state, dispatch] = useReducer(
    createBucketReducer,
    initialBucketState(isRetentionLimitEnforced, org.id)
  )

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

  const retentionRule = state.retentionRules.find(r => r.type === 'expire')
  const retentionSeconds = retentionRule ? retentionRule.everySeconds : 3600

  const handleChangeRuleType = (ruleType: RuleType): void => {
    if (ruleType === 'expire') {
      dispatch({type: 'updateRetentionRules', payload: DEFAULT_RULES})
    } else {
      dispatch({type: 'updateRetentionRules', payload: []})
    }
    dispatch({type: 'updateRuleType', payload: ruleType})
  }

  const handleChangeRetentionRule = (everySeconds: number): void => {
    const retentionRules = [
      {
        type: 'expire',
        everySeconds,
      },
    ]

    dispatch({type: 'updateRetentionRules', payload: retentionRules})
  }

  const handleSubmit = (onHide: () => void) => (
    e: FormEvent<HTMLFormElement>
  ): void => {
    e.preventDefault()

    createBucket(state)
    onHide()
  }

  const handleChangeInput = (e: ChangeEvent<HTMLInputElement>): void => {
    const value = e.target.value

    if (e.target.name === 'name') {
      dispatch({type: 'updateName', payload: value})
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
            name={state.name}
            buttonText="Create"
            disableRenaming={false}
            ruleType={state.ruleType}
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
