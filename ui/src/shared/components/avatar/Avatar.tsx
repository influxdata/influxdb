// Libraries
import React, {SFC} from 'react'
import classnames from 'classnames'

interface Props {
  imageURI: string
  diameterPixels: number
  customClass?: string
}

const Avatar: SFC<Props> = ({imageURI, diameterPixels, customClass}) => (
  <div
    className={classnames('avatar', {[customClass]: customClass})}
    style={{
      width: `${diameterPixels}px`,
      height: `${diameterPixels}px`,
      backgroundImage: `url(${imageURI})`,
    }}
  />
)

export default Avatar
