// Libraries
import React, {FC, useEffect} from 'react'
import {connect, ConnectedProps, useDispatch} from 'react-redux'

// Components
import {Button, IconFont, ComponentColor} from '@influxdata/clockface'

// Actions
import {checkBucketLimits, LimitStatus} from 'src/cloud/actions/limits'
import {showOverlay, dismissOverlay} from 'src/overlays/actions/overlays'

// Utils
import {extractBucketLimits} from 'src/cloud/utils/limits'

// Types
import {AppState} from 'src/types'

// Constants
import {CLOUD} from 'src/shared/constants'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

const CreateBucketButton: FC<Props> = ({
  limitStatus,
  onShowOverlay,
  onDismissOverlay,
}) => {
  const dispatch = useDispatch()
  useEffect(() => {
    // Check bucket limits when component mounts
    dispatch(checkBucketLimits())
  }, [dispatch])

  const limitExceeded = limitStatus === LimitStatus.EXCEEDED

  const handleItemClick = (): void => {
    if (CLOUD && limitExceeded) {
      onShowOverlay('asset-limit', {asset: 'Buckets'}, onDismissOverlay)
    } else {
      onShowOverlay('create-bucket', null, onDismissOverlay)
    }
  }

  return (
    <Button
      icon={IconFont.Plus}
      color={ComponentColor.Primary}
      text="Create Bucket"
      titleText="Click to create a bucket"
      onClick={handleItemClick}
      testID="Create Bucket"
    />
  )
}

const mstp = (state: AppState) => {
  return {
    limitStatus: extractBucketLimits(state.cloud.limits),
  }
}

const mdtp = {
  onShowOverlay: showOverlay,
  onDismissOverlay: dismissOverlay,
}

const connector = connect(mstp, mdtp)

export default connector(CreateBucketButton)
