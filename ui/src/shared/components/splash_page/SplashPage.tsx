import React, {Component} from 'react'

import SplashLogo from 'src/shared/components/splash_page/SplashLogo'
import SplashPanel from 'src/shared/components/splash_page/SplashPanel'
import SplashHeader from 'src/shared/components/splash_page/SplashHeader'

interface Props {
  children: JSX.Element | JSX.Element[]
  panelWidthPixels: number
}

class SplashPage extends Component<Props> {
  public static Logo = SplashLogo
  public static Panel = SplashPanel
  public static Header = SplashHeader

  public render() {
    const {children, panelWidthPixels} = this.props

    return (
      <div className="auth-page">
        <div className="auth-box" style={{width: `${panelWidthPixels}px`}}>
          {children}
        </div>
        <p className="auth-credits">
          Powered by <span className="icon cubo-uniform" />InfluxData
        </p>
        <div className="auth-image" />
      </div>
    )
  }
}

export default SplashPage
