import React, {SFC} from 'react'

import classnames from 'classnames'

import {Bucket} from 'src/types/v2'

export interface DatabaseListItemProps {
  isActive: boolean
  bucket: Bucket
  onChooseNamespace: (b: Bucket) => () => void
}

const DatabaseListItem: SFC<DatabaseListItemProps> = ({
  isActive,
  bucket,
  onChooseNamespace,
}) => (
  <div
    className={classnames('query-builder--list-item', {
      active: isActive,
    })}
    onClick={onChooseNamespace(bucket)}
  >
    {bucket.name}
  </div>
)

export default DatabaseListItem
