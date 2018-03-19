import React, {SFC} from 'react'
import classnames from 'classnames'

interface Props {
  fixed: boolean
  onToggleFixFirstColumn: () => void
}

const GraphOptionsFixFirstColumn: SFC<Props> = ({
  fixed,
  onToggleFixFirstColumn,
}) =>
  <div
    className={classnames('query-builder--list-item', {
      active: fixed,
    })}
    onClick={onToggleFixFirstColumn}
  >
    <span>
      <div
        className="query-builder--checkbox"
        onClick={onToggleFixFirstColumn}
      />
      <label>Fix First Column</label>
    </span>
  </div>

export default GraphOptionsFixFirstColumn
