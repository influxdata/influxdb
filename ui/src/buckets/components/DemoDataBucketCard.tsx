// Libraries
import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'
import CopyToClipboard from 'react-copy-to-clipboard'
import {connect} from 'react-redux'

// Constants
import {
  copyToClipboardSuccess,
  copyToClipboardFailed,
} from 'src/shared/copy/notifications'

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
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {DemoBucket} from 'src/types'

interface DispatchProps {
  removeBucket: typeof deleteDemoDataBucketMembership
  notify: typeof notifyAction
}

interface Props {
  bucket: DemoBucket
}

const DemoDataBucketCard: FC<Props & WithRouterProps & DispatchProps> = ({
  bucket,
  router,
  params: {orgID},
  removeBucket,
  notify,
}) => {
  const handleNameClick = () => {
    router.push(`/orgs/${orgID}/data-explorer?bucket=${bucket.name}`)
  }

  const handleCopyAttempt = (
    copiedText: string,
    isSuccessful: boolean
  ): void => {
    const text = copiedText.slice(0, 30).trimRight()
    const truncatedText = `${text}...`

    if (isSuccessful) {
      notify(copyToClipboardSuccess(truncatedText, 'Bucket ID'))
    } else {
      notify(copyToClipboardFailed(truncatedText, 'Bucket ID'))
    }
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
        <CopyToClipboard text={bucket.id} onCopy={handleCopyAttempt}>
          <span className="copy-bucket-id" title="Click to Copy to Clipboard">
            ID: {bucket.id}
            <span className="copy-bucket-id--helper">Copy to Clipboard</span>
          </span>
        </CopyToClipboard>
      </ResourceCard.Meta>
      <FlexBox
        direction={FlexDirection.Row}
        margin={ComponentSize.Small}
        style={{marginTop: '4px'}}
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
      </FlexBox>
    </ResourceCard>
  )
}

const mdtp: DispatchProps = {
  removeBucket: deleteDemoDataBucketMembership,
  notify: notifyAction,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(withRouter<Props>(DemoDataBucketCard))
