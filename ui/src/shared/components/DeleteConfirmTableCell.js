import React from 'react'

import DeleteConfirmButtons from 'shared/components/DeleteConfirmButtons'
import {ADMIN_TABLE} from 'src/admin/constants/tableSizing'

const DeleteConfirmTableCell = props =>
  <td className="text-right" style={{width: `${ADMIN_TABLE.colDelete}px`}}>
    <DeleteConfirmButtons {...props} />
  </td>

export default DeleteConfirmTableCell
