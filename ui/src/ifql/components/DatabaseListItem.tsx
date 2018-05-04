import React, {PureComponent} from 'react'

import classnames from 'classnames'

import TagList from 'src/ifql/components/TagList'

interface Props {
  db: string
}

interface State {
  isOpen: boolean
}

class DatabaseListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
    }
  }

  public render() {
    const {db} = this.props

    return (
      <div className={this.className} onClick={this.handleChooseDatabase}>
        <div className="ifql-schema-item">
          <span className="icon caret-right" />
          {db}
        </div>
        {this.state.isOpen && <TagList db={db} />}
      </div>
    )
  }

  private get className(): string {
    return classnames('ifql-schema-tree', {
      expanded: this.state.isOpen,
    })
  }

  private handleChooseDatabase = () => {
    this.setState({isOpen: !this.state.isOpen})
  }
}

export default DatabaseListItem
