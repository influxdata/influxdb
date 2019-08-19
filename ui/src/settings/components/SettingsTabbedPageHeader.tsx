// Libraries
import React, {SFC} from 'react'

// Components
import {FlexBox, FlexDirection, JustifyContent} from '@influxdata/clockface'

interface Props {
  children: JSX.Element[] | JSX.Element
}

const SettingsTabbedPageHeader: SFC<Props> = ({children}) => (
  <FlexBox
    direction={FlexDirection.Row}
    justifyContent={JustifyContent.SpaceBetween}
    style={{marginBottom: '32px'}}
  >
    {children}
  </FlexBox>
)

export default SettingsTabbedPageHeader
