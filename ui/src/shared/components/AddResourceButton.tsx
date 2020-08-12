// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import _ from 'lodash'

// Components
import {
  IconFont,
  ComponentColor,
  ComponentSize,
  Button,
  ComponentStatus,
} from '@influxdata/clockface'

// Actions
import {showOverlay, dismissOverlay} from 'src/overlays/actions/overlays'

// Types
import {LimitStatus} from 'src/cloud/actions/limits'

// Constants
import {CLOUD} from 'src/shared/constants'

interface Props {
  onSelectNew: () => void
  resourceName: string
  status?: ComponentStatus
  limitStatus?: LimitStatus
}

type ReduxProps = ConnectedProps<typeof connector>

const AddResourceButton: FC<Props & ReduxProps> = ({
  resourceName,
  onSelectNew,
  onShowOverlay,
  onDismissOverlay,
  limitStatus = LimitStatus.OK,
  status = ComponentStatus.Default,
}) => {
  const showLimitOverlay = () =>
    onShowOverlay('asset-limit', {asset: `${resourceName}s`}, onDismissOverlay)

  const onClick =
    CLOUD && limitStatus === LimitStatus.EXCEEDED
      ? showLimitOverlay
      : onSelectNew

  return (
    <Button
      style={{width: '190px'}}
      testID="add-resource-dropdown--button"
      onClick={onClick}
      color={ComponentColor.Primary}
      size={ComponentSize.Small}
      text={`Create New ${resourceName}`}
      icon={IconFont.Plus}
      status={status}
    />
  )
}

const mdtp = {
  onShowOverlay: showOverlay,
  onDismissOverlay: dismissOverlay,
}

const connector = connect(null, mdtp)

export default connector(AddResourceButton)
