import React, {PropTypes} from 'react'
import TickscriptHeader from 'src/kapacitor/components/TickscriptHeader'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import TickscriptEditor from 'src/kapacitor/components/TickscriptEditor'

const Tickscript = ({source, onSave}) => (
  <div className="page">
    <TickscriptHeader source={source} onSave={onSave} />
    <FancyScrollbar className="page-contents fancy-scroll--kapacitor">
      <div className="container-fluid">
        <div className="row">
          <div className="col-xs-12">
            <TickscriptEditor />
          </div>
        </div>
      </div>
    </FancyScrollbar>
  </div>
)

const {func, shape} = PropTypes

Tickscript.propTypes = {
  onSave: func,
  source: shape(),
}

export default Tickscript
