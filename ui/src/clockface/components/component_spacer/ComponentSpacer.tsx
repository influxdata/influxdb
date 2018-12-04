// Libraries
import React, {SFC} from 'react'
import classnames from 'classnames'

// Types
import {Alignment, Stack} from 'src/clockface'

interface Props {
  children: JSX.Element | JSX.Element[]
  align: Alignment
  stackChildren?: Stack
  stretchToFit?: boolean
}

const ComponentSpacer: SFC<Props> = ({
  children,
  align,
  stackChildren = Stack.Columns,
  stretchToFit = false,
}) => (
  <div
    className={classnames('component-spacer', {
      'component-spacer--left': align === Alignment.Left,
      'component-spacer--center': align === Alignment.Center,
      'component-spacer--right': align === Alignment.Right,
      'component-spacer--horizontal': stackChildren === Stack.Columns,
      'component-spacer--vertical': stackChildren === Stack.Rows,
      'component-spacer--stretch': stretchToFit,
    })}
  >
    {children}
  </div>
)

export default ComponentSpacer
