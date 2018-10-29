// Libraries
import React, {SFC} from 'react'

interface Props {
  children: any
}

const WizardFullScreen: SFC<Props> = (props: Props) => {
  return (
    <>
      <div className="wizard--full-screen">
        {props.children}
        <div className="wizard--credits">
          Powered by <span className="icon cubo-uniform" /> InfluxData
        </div>
      </div>
      <div className="auth-image" />
    </>
  )
}

export default WizardFullScreen
