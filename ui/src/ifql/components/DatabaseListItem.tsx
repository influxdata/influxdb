import React, {PureComponent} from 'react'

import classnames from 'classnames'

export interface Props {
  isActive: boolean
  db: string
  onChooseDatabase: (db: string) => void
}

class DatabaseListItem extends PureComponent<Props> {
  constructor(props) {
    super(props)
    this.state = {
      measurement: '',
    }
  }

  public render() {
    const {db} = this.props

    return (
      <div className={this.className} onClick={this.handleChooseDatabase}>
        {db}
      </div>
    )
  }

  private get className(): string {
    return classnames('query-builder--list-item', {
      active: this.props.isActive,
    })
  }

  private handleChooseDatabase = () => {
    const {onChooseDatabase, db} = this.props
    onChooseDatabase(db)
  }
}

export default DatabaseListItem
