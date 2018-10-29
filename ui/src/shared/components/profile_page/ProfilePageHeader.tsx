// Libraries
import React, {SFC} from 'react'

interface Props {
  children: JSX.Element[] | JSX.Element
}

const ProfilePageHeader: SFC<Props> = ({children}) => (
  <div className="profile-section--header">{children}</div>
)

export default ProfilePageHeader
