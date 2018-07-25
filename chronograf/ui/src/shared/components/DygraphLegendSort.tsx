import React, {PureComponent} from 'react'
import classnames from 'classnames'

interface Props {
  isActive: boolean
  isAscending: boolean
  top: string
  bottom: string
  onSort: () => void
}

class DygraphLegendSort extends PureComponent<Props> {
  public render() {
    const {isAscending, top, bottom, onSort, isActive} = this.props
    return (
      <div
        className={classnames('sort-btn btn btn-xs btn-square', {
          'btn-primary': isActive,
          'btn-default': !isActive,
          'sort-btn--asc': isAscending && isActive,
          'sort-btn--desc': !isAscending && isActive,
        })}
        onClick={onSort}
      >
        <div className="sort-btn--rotator">
          <div className="sort-btn--top">{top}</div>
          <div className="sort-btn--bottom">{bottom}</div>
        </div>
      </div>
    )
  }
}

export default DygraphLegendSort
