import React, {PropTypes} from 'react'
import {Link} from 'react-router'

import SourceIndicator from 'shared/components/SourceIndicator'
import TickscriptType from 'src/kapacitor/components/TickscriptType'
import MultiSelectDBDropdown from 'shared/components/MultiSelectDBDropdown'
import TickscriptID, {
  TickscriptStaticID,
} from 'src/kapacitor/components/TickscriptID'

const addName = list => list.map(l => ({...l, name: `${l.db}.${l.rp}`}))

const TickscriptHeader = ({
  task: {id, type, dbrps},
  task,
  source,
  onSave,
  onChangeType,
  onChangeID,
  onSelectDbrps,
  isNewTickscript,
}) =>
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        {isNewTickscript
          ? <TickscriptID onChangeID={onChangeID} id={id} />
          : <TickscriptStaticID id={task.name} />}
      </div>
      <div className="page-header__right">
        <SourceIndicator />
        <TickscriptType type={type} onChangeType={onChangeType} />
        <MultiSelectDBDropdown
          selectedItems={addName(dbrps)}
          onApply={onSelectDbrps}
        />
        <Link
          className="btn btn-sm btn-default"
          to={`/sources/${source.id}/alert-rules`}
        >
          Cancel
        </Link>
        <button
          className="btn btn-success btn-sm"
          title={id ? '' : 'ID your TICKscript to save'}
          onClick={onSave}
          disabled={!id}
        >
          Save Rule
        </button>
      </div>
    </div>
  </div>

const {arrayOf, bool, func, shape, string} = PropTypes

TickscriptHeader.propTypes = {
  onSave: func,
  source: shape({
    id: string,
  }),
  onSelectDbrps: func.isRequired,
  task: shape({
    dbrps: arrayOf(
      shape({
        db: string,
        rp: string,
      })
    ),
  }),
  onChangeType: func.isRequired,
  onChangeID: func.isRequired,
  isNewTickscript: bool.isRequired,
}

export default TickscriptHeader
