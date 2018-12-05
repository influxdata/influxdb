// Libraries
import React, {SFC} from 'react'

interface Props {
  text: string
}

const SlideToggleLabel: SFC<Props> = ({text}) => (
  <label className="slide-toggle--label">{text}</label>
)

export default SlideToggleLabel
