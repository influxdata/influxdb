// Libraries
import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {
  ResourceCard,
  Label,
  Alignment,
  AlignItems,
  ComponentSize,
  ComponentColor,
  ButtonShape,
  FlexDirection,
  FlexBox,
  IconFont,
} from '@influxdata/clockface'
import {Context} from 'src/clockface'

// Actions
import {deleteDemoDataBucketMembership} from 'src/cloud/actions/demodata'

// Types
import {DemoBucket} from 'src/types'

interface DispatchProps {
  removeBucket: typeof deleteDemoDataBucketMembership
}

interface Props {
  bucket: DemoBucket
}

const DemoDataBucketCard: FC<Props & WithRouterProps & DispatchProps> = ({
  bucket,
  router,
  params: {orgID},
  removeBucket,
}) => {
  const handleNameClick = () => {
    router.push(`/orgs/${orgID}/data-explorer?bucket=${bucket.name}`)
  }

  return (
    <ResourceCard
      testID={`bucket-card ${bucket.name}`}
      contextMenu={
        <Context align={Alignment.Center}>
          <FlexBox
            alignItems={AlignItems.Center}
            direction={FlexDirection.Row}
            margin={ComponentSize.Small}
          >
            <Context.Menu
              icon={IconFont.Trash}
              color={ComponentColor.Danger}
              shape={ButtonShape.Default}
              text="Delete Bucket"
              testID={`context-delete-menu ${bucket.name}`}
            >
              <Context.Item
                label="Confirm"
                action={removeBucket}
                value={bucket}
                testID={`context-delete-bucket ${bucket.name}`}
              />
            </Context.Menu>
          </FlexBox>
        </Context>
      }
    >
      <ResourceCard.Name
        testID={`bucket--card--name ${bucket.name}`}
        onClick={handleNameClick}
        name={bucket.name}
      />
      <ResourceCard.Meta>
        <span
          className="system-bucket"
          key={`system-bucket-indicator-${bucket.name}`}
        >
          Demo Data Bucket
        </span>
        <>Retention: {bucket.readableRetention}</>
      </ResourceCard.Meta>
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

const mdtp: DispatchProps = {
  removeBucket: deleteDemoDataBucketMembership,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(withRouter<Props>(DemoDataBucketCard))
