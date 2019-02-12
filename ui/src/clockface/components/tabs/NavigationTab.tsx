// Libraries
import React, {SFC} from 'react'
import classnames from 'classnames'
import {Link} from 'react-router'

interface Props {
  id: string
  title: string
  active: boolean
  url: string
}

const NavigationTab: SFC<Props> = ({title, active, url}) => (
  <Link className={classnames('tabs--tab', {active})} to={url}>
    {title}
  </Link>
)

export default NavigationTab
