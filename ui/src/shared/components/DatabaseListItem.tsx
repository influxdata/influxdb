import React, {SFC} from 'react'

import classnames from 'classnames'

import {Namespace} from 'src/types/query'

export interface DatabaseListItemProps {
  isActive: boolean
  namespace: Namespace
  onChooseNamespace: (namespace: Namespace) => () => void
}

const DatabaseListItem: SFC<DatabaseListItemProps> = ({
  isActive,
  namespace,
  namespace: {database, retentionPolicy},
  onChooseNamespace,
}) => (
  <div
    className={classnames('query-builder--list-item', {
      active: isActive,
    })}
    onClick={onChooseNamespace(namespace)}
  >
    {database}.{retentionPolicy}
  </div>
)

export default DatabaseListItem
