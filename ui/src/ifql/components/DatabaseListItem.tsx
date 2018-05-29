import React, {PureComponent} from 'react'
import classnames from 'classnames'

import {tagKeys as fetchTagKeys} from 'src/shared/apis/v2/metaQueries'
import parseValuesColumn from 'src/shared/parsing/v2/tags'
import TagList from 'src/ifql/components/TagList'
import {Service} from 'src/types'

interface Props {
  db: string
  service: Service
}

interface State {
  isOpen: boolean
  tags: string[]
}

class DatabaseListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
      tags: [],
    }
  }

  public async componentDidMount() {
    const {db, service} = this.props

    try {
      const response = await fetchTagKeys(service, db, [])
      const tags = parseValuesColumn(response)
      this.setState({tags})
    } catch (error) {
      console.error(error)
    }
  }

  public render() {
    const {db, service} = this.props
    const {tags} = this.state

    return (
      <div className={this.className} onClick={this.handleChooseDatabase}>
        <div className="ifql-schema-item">
          <div className="ifql-schema-item-toggle" />
          {db}
          <span className="ifql-schema-type">Bucket</span>
        </div>
        {this.state.isOpen && (
          <>
            <TagList db={db} service={service} tags={tags} filter={[]} />
          </>
        )}
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
