// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Grid} from '@influxdata/clockface'
import ColorSelector from 'src/timeMachine/components/view_options/ColorSelector'

// Actions
import {setColors} from 'src/timeMachine/actions'

// Types
import {ViewType} from 'src/types'
import {Color} from 'src/types/colors'

interface OwnProps {
  type: ViewType
  colors: Color[]
}

interface DispatchProps {
  onUpdateColors: (colors: Color[]) => void
}

type Props = OwnProps & DispatchProps

class ScatterOptions extends PureComponent<Props> {
  public render() {
    const {colors, onUpdateColors} = this.props

    return (
      <>
        <Grid.Column>
          <h4 className="view-options--header">Customize Graph</h4>
        </Grid.Column>
        <ColorSelector
          colors={colors.filter(c => c.type === 'scale')}
          onUpdateColors={onUpdateColors}
        />
      </>
    )
  }
}

const mdtp: DispatchProps = {
  onUpdateColors: setColors,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(ScatterOptions)
