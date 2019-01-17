// Libraries
import React, {SFC} from 'react'

// Components
import {ComponentSpacer, Stack, Alignment} from 'src/clockface'

interface Props {
  children: JSX.Element | JSX.Element[]
}

const OverlayFooter: SFC<Props> = ({children}) => (
  <div className="overlay--footer">
    <ComponentSpacer
      stackChildren={Stack.Columns}
      align={Alignment.Center}
      stretchToFitWidth={true}
    >
      {children}
    </ComponentSpacer>
  </div>
)

export default OverlayFooter
