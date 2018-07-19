import React, {ReactNode, SFC} from 'react'

interface Props {
  children: ReactNode
}

const CEOBottom: SFC<Props> = ({children}) => (
  <div className="ceo--editor">{children}</div>
)

export default CEOBottom
