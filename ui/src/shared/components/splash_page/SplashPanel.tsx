import React, {SFC} from 'react'

interface Props {
  children: JSX.Element[] | JSX.Element
}

const SplashPanel: SFC<Props> = ({children}) => (
  <div className="splash-page--panel">{children}</div>
)

export default SplashPanel
