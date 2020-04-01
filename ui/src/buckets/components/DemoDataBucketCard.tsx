// Libraries
import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import {ResourceCard} from '@influxdata/clockface'

// Types
import {DemoBucket} from 'src/types'

interface Props {
  bucket: DemoBucket
}

const DemoDataBucketCard: FC<Props & WithRouterProps> = ({
  bucket,
  router,
  params: {orgID},
}) => {
  const handleNameClick = () => {
    router.push(`/orgs/${orgID}/data-explorer?bucket=${bucket.name}`)
  }

  return (
    <ResourceCard
      testID={`bucket-card ${bucket.name}`}
      name={
        <ResourceCard.Name
          testID={`bucket--card--name ${bucket.name}`}
          onClick={handleNameClick}
          name={bucket.name}
        />
      }
      metaData={[
        <span
          className="demod-bucket"
          key={`system-bucket-indicator-${bucket.id}`}
        >
          Demo Data Bucket
        </span>,
        <>Retention: {bucket.readableRetention}</>,
      ]}
    />
  )
}

export default withRouter<Props>(DemoDataBucketCard)
