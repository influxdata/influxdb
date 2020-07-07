// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {SquareButton, IconFont} from '@influxdata/clockface'

// Actions
import {delayEnablePresentationMode} from 'src/shared/actions/app'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

const PresentationModeToggle: FC<Props> = ({handleClickPresentationButton}) => (
  <SquareButton
    icon={IconFont.ExpandA}
    testID="presentation-mode-toggle"
    onClick={handleClickPresentationButton}
  />
)

const mdtp = {
  handleClickPresentationButton: delayEnablePresentationMode,
}

const connector = connect(null, mdtp)

export default connector(PresentationModeToggle)
