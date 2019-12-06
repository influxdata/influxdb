// Libraries
import React, {FC} from 'react'
import {BorderType, ComponentSize, Table} from '@influxdata/clockface'

interface Props {
  bodyData: string[]
  headers: string[]
}

const PreviewDataTable: FC<Props> = ({bodyData, headers}) => {
  return (
    <Table
      borders={BorderType.Vertical}
      fontSize={ComponentSize.ExtraSmall}
      cellPadding={ComponentSize.ExtraSmall}
    >
      <Table.Header>
        <Table.Row>
          <Table.HeaderCell>Columns</Table.HeaderCell>
          <Table.HeaderCell>Sample Values</Table.HeaderCell>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {headers.map((header, i) => (
          <Table.Row key={header}>
            <Table.Cell>{header}</Table.Cell>
            <Table.Cell>{bodyData[i]}</Table.Cell>
          </Table.Row>
        ))}
      </Table.Body>
    </Table>
  )
}

export default PreviewDataTable
