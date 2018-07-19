import React, {SFC} from 'react'

interface Props {
  tableName: string
}
const EmptyRow: SFC<Props> = ({tableName}) => (
  <tr className="table-empty-state">
    <th colSpan={5}>
      <p>
        You don't have any {tableName},<br />why not create one?
      </p>
    </th>
  </tr>
)

export default EmptyRow
