import React, {PropTypes} from 'react'
import TickscriptHeader from 'src/kapacitor/components/TickscriptHeader'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import TickscriptEditor from 'src/kapacitor/components/TickscriptEditor'

const Tickscript = ({
  source,
  onSave,
  task,
  validation,
  onSelectDbrps,
  onChangeScript,
}) => (
  <div className="page">
    <TickscriptHeader
      task={task}
      source={source}
      onSelectDbrps={onSelectDbrps}
      onSave={onSave}
    />
    <FancyScrollbar className="page-contents fancy-scroll--kapacitor">
      <div className="container-fluid">
        <div className="row">
          <div className="col-xs-12">
            {validation}
          </div>
        </div>
        <div className="row">
          <div className="col-xs-12">
            <TickscriptEditor
              script={task.script}
              onChangeScript={onChangeScript}
            />
          </div>
        </div>
      </div>
    </FancyScrollbar>
  </div>
)

const {arrayOf, func, shape, string} = PropTypes

Tickscript.propTypes = {
  onSave: func.isRequired,
  source: shape(),
  task: shape({
    id: string,
    script: string,
    dbsrps: arrayOf(shape()),
  }).isRequired,
  onChangeScript: func.isRequired,
  onSelectDbrps: func.isRequired,
  validation: string,
}

export default Tickscript
