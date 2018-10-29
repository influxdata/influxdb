// Libraries
import React, {SFC} from 'react'
import classnames from 'classnames'

// Types
import {Alignment} from 'src/clockface'

interface Props {
  children: JSX.Element | JSX.Element[]
  align: Alignment
}

const ComponentSpacer: SFC<Props> = ({children, align}) => (
  <div
    className={classnames('component-spacer', {
      'component-spacer--left': align === Alignment.Left,
      'component-spacer--center': align === Alignment.Center,
      'component-spacer--right': align === Alignment.Right,
    })}
  >
    {children}
  </div>
)

export default ComponentSpacer
