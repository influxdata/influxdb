// Libraries
import React, {FC, ChangeEvent, FormEvent, useReducer} from 'react'
import {connect} from 'react-redux'

// Components
import {Overlay} from '@influxdata/clockface'
import BucketOverlayForm from 'src/buckets/components/BucketOverlayForm'

// Utils
import {extractBucketMaxRetentionSeconds} from 'src/cloud/utils/limits'

// Actions
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
  const [state, dispatch] = useReducer(
    createBucketReducer,
    initialBucketState(isRetentionLimitEnforced, org.id)
  )

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

  const handleSubmit = (e: FormEvent<HTMLFormElement>): void => {
    e.preventDefault()

    createBucket(state)
    onClose()
  }

  const handleChangeInput = (e: ChangeEvent<HTMLInputElement>): void => {
    const value = e.target.value

    if (e.target.name === 'name') {
      dispatch({type: 'updateName', payload: value})
    }
  }

  return (
    <Overlay.Container maxWidth={400}>
      <Overlay.Header title="Create Bucket" onDismiss={onClose} />
      <Overlay.Body>
        <BucketOverlayForm
          name={state.name}
          buttonText="Create"
          disableRenaming={false}
          ruleType={state.ruleType}
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
