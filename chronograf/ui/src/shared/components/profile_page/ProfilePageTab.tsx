// Libraries
import React, {SFC} from 'react'
import classnames from 'classnames'

interface Props {
  id: string
  title: string
  active: boolean
  url: string
  onClick: (url: string) => () => void
}

const ProfilePageTab: SFC<Props> = ({title, active, url, onClick}) => (
  <div
    className={classnames('profile-nav--tab', {active})}
    onClick={onClick(url)}
  >
    {title}
  </div>
)

export default ProfilePageTab
