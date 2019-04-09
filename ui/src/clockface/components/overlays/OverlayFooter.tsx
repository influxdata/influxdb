// Libraries
import React, {SFC} from 'react'

// Components
import {
  ComponentSpacer,
  FlexDirection,
  JustifyContent,
  AlignItems,
  ComponentSize,
} from '@influxdata/clockface'

interface Props {
  children: JSX.Element | JSX.Element[]
}

const OverlayFooter: SFC<Props> = ({children}) => (
  <div className="overlay--footer">
    <ComponentSpacer
      margin={ComponentSize.Small}
      direction={FlexDirection.Row}
      justifyContent={JustifyContent.Center}
      alignItems={AlignItems.Center}
      stretchToFitWidth={true}
    >
      {children}
    </ComponentSpacer>
  </div>
)

export default OverlayFooter
