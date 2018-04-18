import React, {ReactNode, SFC} from 'react'

interface Props {
  children: ReactNode
}

const CEOBottom: SFC<Props> = ({children}) => (
  <div className="overlay-technology--editor">{children}</div>
)

export default CEOBottom
