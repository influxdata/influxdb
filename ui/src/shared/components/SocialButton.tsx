// Libraries
import React, {FC} from 'react'
import {ButtonBase, ButtonShape, ComponentSize} from '@influxdata/clockface'

interface Props {
  buttonText: string
  children: JSX.Element
  handleClick?: () => void
}

export const SocialButton: FC<Props> = ({
  buttonText,
  children,
  handleClick,
}) => {
  return (
    <ButtonBase
      onClick={handleClick}
      size={ComponentSize.Large}
      shape={ButtonShape.StretchToFit}
    >
      {children}
      <span className="signup-text">{buttonText}</span>
    </ButtonBase>
  )
}
