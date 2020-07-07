// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {Button, IconFont, ComponentColor} from '@influxdata/clockface'

// Actions
import {toggleVisOptions} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  isViewingVisOptions: boolean
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = StateProps & ReduxProps

export const VisOptionsButton: FC<Props> = ({
  isViewingVisOptions,
  onToggleVisOptions,
}) => {
  const color = isViewingVisOptions
    ? ComponentColor.Primary
    : ComponentColor.Default

  return (
    <Button
      color={color}
      icon={IconFont.CogThick}
      onClick={onToggleVisOptions}
      testID="cog-cell--button"
      text="Customize"
    />
  )
}

const mstp = (state: AppState) => {
  const {isViewingVisOptions} = getActiveTimeMachine(state)

  return {isViewingVisOptions}
}

const mdtp = {
  onToggleVisOptions: toggleVisOptions,
}

const connector = connect(mstp, mdtp)

export default connector(VisOptionsButton)
