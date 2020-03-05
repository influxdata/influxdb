// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {SquareButton, IconFont} from '@influxdata/clockface'

// Actions
import {delayEnablePresentationMode} from 'src/shared/actions/app'

interface DispatchProps {
  handleClickPresentationButton: typeof delayEnablePresentationMode
}

const GraphTips: FC<DispatchProps> = ({handleClickPresentationButton}) => (
  <SquareButton
    icon={IconFont.ExpandA}
    testID="presentation-mode-toggle"
    onClick={handleClickPresentationButton}
  />
)

const mdtp: DispatchProps = {
  handleClickPresentationButton: delayEnablePresentationMode,
}

export default connect<{}, DispatchProps, {}>(null, mdtp)(GraphTips)
