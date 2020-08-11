// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {
  Table,
  ComponentSize,
  ButtonShape,
  ComponentColor,
  IconFont,
  Alignment,
} from '@influxdata/clockface'
import CopyButton from 'src/shared/components/CopyButton'

// Utils
import {getAll} from 'src/resources/selectors'

// Types
import {AppState, ResourceType, Bucket} from 'src/types'

type Props = ConnectedProps<typeof connector>

const WriteDataHelperBuckets: FC<Props> = ({buckets}) => {
  return (
    <Table fontSize={ComponentSize.Small}>
      <Table.Header>
        <Table.Row>
          <Table.HeaderCell style={{width: '80%'}}>Buckets</Table.HeaderCell>
          <Table.HeaderCell style={{width: '20%'}} />
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {buckets.map(bucket => {
          return (
            <Table.Row key={bucket.id}>
              <Table.Cell className="write-data--helper-bucket">
                {bucket.name}
              </Table.Cell>
              <Table.Cell horizontalAlignment={Alignment.Right}>
                <CopyButton
                  shape={ButtonShape.Square}
                  icon={IconFont.Duplicate}
                  size={ComponentSize.ExtraSmall}
                  color={ComponentColor.Default}
                  contentName={bucket.description}
                  textToCopy={bucket.name}
                  testID={`copy-bucket ${bucket.name}`}
                />
              </Table.Cell>
            </Table.Row>
          )
        })}
      </Table.Body>
    </Table>
  )
}

const mstp = (state: AppState) => ({
  buckets: getAll<Bucket>(state, ResourceType.Buckets),
})

const connector = connect(mstp)

export default connector(WriteDataHelperBuckets)
