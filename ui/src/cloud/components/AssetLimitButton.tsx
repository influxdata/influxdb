// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {Button, ComponentColor, IconFont} from '@influxdata/clockface'

// Actions
import {showOverlay, dismissOverlay} from 'src/overlays/actions/overlays'

interface OwnProps {
  color?: ComponentColor
  resourceName: string
  buttonText?: string
}

type ReduxProps = ConnectedProps<typeof connector>

const AssetLimitButton: FC<OwnProps & ReduxProps> = ({
  color = ComponentColor.Primary,
  buttonText,
  resourceName,
  onShowOverlay,
  onDismissOverlay,
}) => {
  const handleClick = () => {
    onShowOverlay('asset-limit', {asset: `${resourceName}s`}, onDismissOverlay)
  }
  return (
    <Button
      icon={IconFont.Plus}
      text={buttonText || `Create ${resourceName}`}
      color={color}
      titleText={`Click to create ${resourceName}`}
      onClick={handleClick}
      testID={`create-${resourceName.toLowerCase()}`}
    />
  )
}

const mdtp = {
  onShowOverlay: showOverlay,
  onDismissOverlay: dismissOverlay,
}

const connector = connect(null, mdtp)

export default connector(AssetLimitButton)
