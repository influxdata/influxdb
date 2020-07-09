// Libraries
import React, {FC, useEffect} from 'react'
import {connect, ConnectedProps, useDispatch} from 'react-redux'

// Components
import {
  Button,
  IconFont,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

// Actions
import {checkBucketLimits, LimitStatus} from 'src/cloud/actions/limits'
import {showOverlay, dismissOverlay} from 'src/overlays/actions/overlays'

// Utils
import {extractBucketLimits} from 'src/cloud/utils/limits'

// Types
import {AppState} from 'src/types'

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
  const text = 'Create Bucket'
  let titleText = 'Click to create a bucket'
  let buttonStatus = ComponentStatus.Default

  if (limitExceeded) {
    titleText = 'This account has the maximum number of buckets allowed'
    buttonStatus = ComponentStatus.Disabled
  }

  const handleItemClick = (): void => {
    if (limitExceeded) {
      return
    }

    onShowOverlay('create-bucket', null, onDismissOverlay)
  }

  return (
    <Button
      icon={IconFont.Plus}
      color={ComponentColor.Primary}
      text={text}
      titleText={titleText}
      onClick={handleItemClick}
      testID="Create Bucket"
      status={buttonStatus}
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
