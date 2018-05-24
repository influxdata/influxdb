import React, {PureComponent} from 'react'
import classnames from 'classnames'

import {measurements as measurementsAsync} from 'src/shared/apis/v2/metaQueries'
import parseMeasurements from 'src/shared/parsing/v2/measurements'
import MeasurementList from 'src/ifql/components/MeasurementList'
import {Service} from 'src/types'

interface Props {
  db: string
  service: Service
}

interface State {
  isOpen: boolean
  measurements: string[]
}

class DatabaseListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
      measurements: [],
    }
  }

  public async componentDidMount() {
    const {db, service} = this.props

    try {
      const response = await measurementsAsync(service, db)
      const measurements = parseMeasurements(response)
      this.setState({measurements})
    } catch (error) {
      console.error(error)
    }
  }

  public render() {
    const {db} = this.props
    const {measurements} = this.state

    return (
      <div className={this.className} onClick={this.handleChooseDatabase}>
        <div className="ifql-schema-item">
          <div className="ifql-schema-item-toggle" />
          {db}
          <span className="ifql-schema-type">Bucket</span>
        </div>
        {this.state.isOpen && <MeasurementList measurements={measurements} />}
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
