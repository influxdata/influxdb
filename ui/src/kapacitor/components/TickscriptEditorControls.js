import React, {PropTypes} from 'react'
import TickscriptType from 'src/kapacitor/components/TickscriptType'
import MultiSelectDBDropdown from 'shared/components/MultiSelectDBDropdown'
import TickscriptID, {
  TickscriptStaticID,
} from 'src/kapacitor/components/TickscriptID'

const addName = list => list.map(l => ({...l, name: `${l.db}.${l.rp}`}))

const TickscriptEditorControls = ({
  isNewTickscript,
  onSelectDbrps,
  onChangeType,
  onChangeID,
  task,
}) =>
  <div className="tickscript-controls">
    {isNewTickscript
      ? <TickscriptID onChangeID={onChangeID} id={task.id} />
      : <TickscriptStaticID id={task.name} />}
    <div className="tickscript-controls--right">
      <TickscriptType type={task.type} onChangeType={onChangeType} />
      <MultiSelectDBDropdown
        selectedItems={addName(task.dbrps)}
        onApply={onSelectDbrps}
      />
    </div>
  </div>

const {arrayOf, bool, func, shape, string} = PropTypes

TickscriptEditorControls.propTypes = {
  isNewTickscript: bool.isRequired,
  onSelectDbrps: func.isRequired,
  onChangeType: func.isRequired,
  onChangeID: func.isRequired,
  task: shape({
    id: string,
    script: string,
    dbsrps: arrayOf(shape()),
  }).isRequired,
}

export default TickscriptEditorControls
