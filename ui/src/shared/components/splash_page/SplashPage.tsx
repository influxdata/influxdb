import React, {Component} from 'react'

import SplashLogo from 'src/shared/components/splash_page/SplashLogo'
import SplashPanel from 'src/shared/components/splash_page/SplashPanel'
import SplashHeader from 'src/shared/components/splash_page/SplashHeader'

interface Props {
  children: JSX.Element | JSX.Element[]
}

class SplashPage extends Component<Props> {
  public static Logo = SplashLogo
  public static Panel = SplashPanel
  public static Header = SplashHeader

  public render() {
    const {children} = this.props

    return (
      <div className="splash-page">
        <div className="splash-page--child">{children}</div>
        <p className="splash-page--credits">
          Powered by <span className="icon cubo-uniform" />{' '}
          <a href="https://www.influxdata.com/" target="_blank">
            InfluxData
          </a>
        </p>
        <div className="splash-page--image" />
      </div>
    )
  }
}

export default SplashPage
