import React, {SFC, ReactNode} from 'react'

interface Props {
  children: ReactNode
}

const OverlayBody: SFC<Props> = ({children}) => (
  <div className="overlay--body">{children}</div>
)

export default OverlayBody
