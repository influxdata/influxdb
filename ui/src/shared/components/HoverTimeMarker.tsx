import React, {SFC} from 'react'

interface Props {
  x: number
}

const HoverTimeMarker: SFC<Props> = props => {
  const style = {left: `${props.x}px`}

  return <div className="hover-time-marker" style={style} />
}

export default HoverTimeMarker
