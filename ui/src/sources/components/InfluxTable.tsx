import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import {SetActiveKapacitor, DeleteKapacitor} from 'src/shared/actions/sources'

import InfluxTableHead from 'src/sources/components/InfluxTableHead'
import InfluxTableHeader from 'src/sources/components/InfluxTableHeader'
import InfluxTableRow from 'src/sources/components/InfluxTableRow'

import {Source, Me} from 'src/types'

interface Props {
  me: Me
  source: Source
  sources: Source[]
  isUsingAuth: boolean
  deleteKapacitor: DeleteKapacitor
  setActiveKapacitor: SetActiveKapacitor
  onDeleteSource: (source: Source) => void
}

class InfluxTable extends PureComponent<Props> {
  public render() {
    const {
      source,
      sources,
      setActiveKapacitor,
      onDeleteSource,
      deleteKapacitor,
      isUsingAuth,
      me,
    } = this.props

    return (
      <div className="row">
        <div className="col-md-12">
          <div className="panel">
            <InfluxTableHeader
              me={me}
              source={source}
              isUsingAuth={isUsingAuth}
            />
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
                        deleteKapacitor={deleteKapacitor}
                        setActiveKapacitor={setActiveKapacitor}
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

const mapStateToProps = ({auth: {isUsingAuth, me}}) => ({isUsingAuth, me})

export default connect(mapStateToProps)(InfluxTable)
