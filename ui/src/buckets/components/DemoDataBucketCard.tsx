// Libraries
import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ResourceCard, Label} from '@influxdata/clockface'

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
          className="system-bucket"
          key={`system-bucket-indicator-${bucket.id}`}
        >
          Demo Data Bucket
        </span>,
        <>Retention: {bucket.readableRetention}</>,
      ]}
    >
      <Label
        id="1"
        key="1"
        name="No Cost"
        color="#757888"
        description=""
        onDelete={null}
        onClick={null}
      />
    </ResourceCard>
  )
}

export default withRouter<Props>(DemoDataBucketCard)
