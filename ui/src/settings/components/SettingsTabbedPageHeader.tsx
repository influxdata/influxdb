// Libraries
import React, {SFC} from 'react'

// Components
import {FlexBox, FlexDirection, JustifyContent} from '@influxdata/clockface'

interface Props {
  children: JSX.Element[] | JSX.Element
  className?: string
}

const SettingsTabbedPageHeader: SFC<Props> = ({children, className}) => (
  <FlexBox
    direction={FlexDirection.Row}
    justifyContent={JustifyContent.SpaceBetween}
    style={{marginBottom: '32px'}}
    className={className}
  >
    {children}
  </FlexBox>
)

export default SettingsTabbedPageHeader
