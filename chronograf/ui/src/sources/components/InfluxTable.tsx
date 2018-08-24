import React, {PureComponent} from 'react'

import InfluxTableHead from 'src/sources/components/InfluxTableHead'
import InfluxTableHeader from 'src/sources/components/InfluxTableHeader'
import InfluxTableRow from 'src/sources/components/InfluxTableRow'

import {Source} from 'src/types/v2'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  source: Source
  sources: Source[]
  onDeleteSource: (source: Source) => void
}

@ErrorHandling
class InfluxTable extends PureComponent<Props> {
  public render() {
    const {source, sources, onDeleteSource} = this.props

    return (
      <div className="row">
        <div className="col-md-12">
          <div className="panel">
            <InfluxTableHeader source={source} />
            <div className="panel-body">
              <table className="table v-center margin-bottom-zero table-highlight">
                <InfluxTableHead />
                <tbody>
                  {sources.map(s => {
                    return (
                      <InfluxTableRow
                        key={s.id}
                        source={s}
                        currentSource={source}
                        onDeleteSource={onDeleteSource}
                      />
                    )
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    )
  }
}

export default InfluxTable
