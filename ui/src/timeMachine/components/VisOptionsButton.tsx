// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

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

interface DispatchProps {
  onToggleVisOptions: typeof toggleVisOptions
}

type Props = StateProps & DispatchProps

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

const mstp = (state: AppState): StateProps => {
  const {isViewingVisOptions} = getActiveTimeMachine(state)

  return {isViewingVisOptions}
}

const mdtp: DispatchProps = {
  onToggleVisOptions: toggleVisOptions,
}

export default connect<StateProps, DispatchProps>(mstp, mdtp)(VisOptionsButton)
