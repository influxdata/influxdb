// Libraries
import React, {SFC} from 'react'
import classnames from 'classnames'

// Types
import {Alignment, Direction} from 'src/clockface'

interface Props {
  children: JSX.Element | JSX.Element[]
  align: Alignment
  direction?: Direction
  stretchToFit?: boolean
}

const ComponentSpacer: SFC<Props> = ({
  children,
  align,
  direction = Direction.Horizontal,
  stretchToFit = false,
}) => (
  <div
    className={classnames('component-spacer', {
      'component-spacer--left': align === Alignment.Left,
      'component-spacer--center': align === Alignment.Center,
      'component-spacer--right': align === Alignment.Right,
      'component-spacer--horizontal': direction === Direction.Horizontal,
      'component-spacer--vertical': direction === Direction.Vertical,
      'component-spacer--stretch': stretchToFit,
    })}
  >
    {children}
  </div>
)

export default ComponentSpacer
