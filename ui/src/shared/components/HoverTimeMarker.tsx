import React, {SFC} from 'react'

import 'src/shared/components/HoverTimeMarker.scss'

const MARGIN_LEFT = 10

interface Props {
  x: number
}

const HoverTimeMarker: SFC<Props> = props => {
  const style = {left: `${props.x + MARGIN_LEFT}px`}

  return <div className="hover-time-marker" style={style} />
}

export default HoverTimeMarker
