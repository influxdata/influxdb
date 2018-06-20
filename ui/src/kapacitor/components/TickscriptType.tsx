import React, {PureComponent, MouseEvent} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  type: string
  onChangeType: (type: string) => (event: MouseEvent<HTMLLIElement>) => void
}

const STREAM = 'stream'
const BATCH = 'batch'

@ErrorHandling
class TickscriptType extends PureComponent<Props> {
  public render() {
    const {onChangeType} = this.props
    return (
      <ul className="nav nav-tablist nav-tablist-sm">
        <li
          className={this.getClassName(STREAM)}
          onClick={onChangeType(STREAM)}
        >
          Stream
        </li>
        <li className={this.getClassName(BATCH)} onClick={onChangeType(BATCH)}>
          Batch
        </li>
      </ul>
    )
  }

  private getClassName(type: string) {
    if (type === this.props.type) {
      return 'active'
    }
    return ''
  }
}

export default TickscriptType
