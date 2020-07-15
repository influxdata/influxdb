// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router-dom'

// Components
import {
  TreeNav,
  IconFont,
  InfluxDBCloudLogo,
  Icon,
  ComponentColor,
} from '@influxdata/clockface'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'

interface Props {
  link: string
}

const NavHeader: FC<Props> = ({link}) => {
  return (
    <>
      <CloudExclude>
        <TreeNav.Header
          id="getting-started"
          icon={<Icon glyph={IconFont.CuboNav} />}
          label={<InfluxDBCloudLogo cloud={false} />}
          color={ComponentColor.Secondary}
          linkElement={className => <Link className={className} to={link} />}
        />
      </CloudExclude>
      <CloudOnly>
        <TreeNav.Header
          id="getting-started"
          icon={<Icon glyph={IconFont.CuboNav} />}
          label={<InfluxDBCloudLogo cloud={true} />}
          color={ComponentColor.Primary}
          linkElement={className => <Link className={className} to={link} />}
        />
      </CloudOnly>
    </>
  )
}

export default NavHeader
