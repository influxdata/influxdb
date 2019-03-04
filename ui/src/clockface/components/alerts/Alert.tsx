// Libraries
import React, {SFC} from 'react'
import classnames from 'classnames'

// Types
import {ComponentColor, IconFont} from '@influxdata/clockface'

// Styles
import 'src/clockface/components/alerts/Alert.scss'

interface Props {
  children: JSX.Element | JSX.Element[]
  color: ComponentColor
  icon?: IconFont
}

const Alert: SFC<Props> = ({children, color, icon}) => {
  const className = classnames('alert', {
    [`alert--${color}`]: color,
    'alert--has-icon': icon,
  })

  return (
    <div className={className}>
      {icon && <span className={`alert--icon icon ${icon}`} />}
      <div className="alert--contents">{children}</div>
    </div>
  )
}

export default Alert
