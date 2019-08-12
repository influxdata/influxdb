// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {Form, Grid} from '@influxdata/clockface'

// Components
import HexColorSchemeDropdown from 'src/shared/components/HexColorSchemeDropdown'

// Actions
import {setColorHexes} from 'src/timeMachine/actions'

// Constants
import {GIRAFFE_COLOR_SCHEMES} from 'src/shared/constants'

// Types
import {CheckViewProperties} from 'src/types'

interface OwnProps {
  properties: CheckViewProperties
}

interface DispatchProps {
  onSetColors: typeof setColorHexes
}

type Props = OwnProps & DispatchProps

const CheckOptions: FC<Props> = ({properties: {colors}, onSetColors}) => {
  return (
    <Grid.Column>
      <Form.Element label="Color Scheme">
        <HexColorSchemeDropdown
          colorSchemes={GIRAFFE_COLOR_SCHEMES}
          selectedColorScheme={colors}
          onSelectColorScheme={onSetColors}
        />
      </Form.Element>
    </Grid.Column>
  )
}

const mdtp = {
  onSetColors: setColorHexes,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(CheckOptions)
