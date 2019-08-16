// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, ComponentColor, IconFont} from '@influxdata/clockface'

// Actions
import {convertFromCheckView} from 'src/timeMachine/actions'

interface DispatchProps {
  onConvertFromCheckView: typeof convertFromCheckView
}

type Props = DispatchProps

const RemoveButton: FunctionComponent<Props> = ({onConvertFromCheckView}) => {
  const handleClick = () => {
    onConvertFromCheckView()
  }

  return (
    <Button
      titleText="Remove Check from Cell"
      text="Remove Check from Cell"
      onClick={handleClick}
      icon={IconFont.Remove}
      color={ComponentColor.Danger}
    />
  )
}

const mdtp: DispatchProps = {
  onConvertFromCheckView: convertFromCheckView,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(RemoveButton)
