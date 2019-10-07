// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  IconFont,
  ComponentSize,
  ComponentColor,
  Button,
  ComponentStatus,
} from '@influxdata/clockface'
import OverlayLink from 'src/overlays/components/OverlayLink'

// Actions
import {LimitStatus} from 'src/cloud/actions/limits'

// Utils
import {extractBucketLimits} from 'src/cloud/utils/limits'

// Types
import {AppState} from 'src/types'

interface OwnProps {
  color?: ComponentColor
  size?: ComponentSize
}

interface StateProps {
  limitStatus: LimitStatus
}

type Props = OwnProps & StateProps

const CreateBucketButton: FunctionComponent<Props> = ({color = ComponentColor.Primary, size = ComponentSize.Small, limitStatus}) => {
  const disabled = limitStatus === LimitStatus.EXCEEDED

  const titleText = disabled ? 'This account has the maximum number of buckets allowed' : 'Create a bucket'
  const status = disabled ? ComponentStatus.Disabled : ComponentStatus.Default

  return (
    <OverlayLink overlayID="create-bucket">
      {onClick => (
        <Button
          text="Create Bucket"
          icon={IconFont.Plus}
          size={size}
          color={color}
          onClick={onClick}
          testID="Create Bucket"
          status={status}
          titleText={titleText}
        />
      )}
    </OverlayLink>
  )
}

const mstp = ({cloud: {limits}}: AppState): StateProps => ({
  limitStatus: extractBucketLimits(limits),
})

export default connect<StateProps, {}, {}>(
  mstp
)(CreateBucketButton)