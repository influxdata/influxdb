// Libraries
import React, {SFC} from 'react'
import classnames from 'classnames'

// Components
import CellHeaderNote from 'src/shared/components/cells/CellHeaderNote'

interface Props {
  name: string
  note: string
}

const CellHeader: SFC<Props> = ({name, note}) => {
  const className = classnames('cell--header cell--draggable', {
    'cell--header-note': !!note,
  })

  return (
    <div className={className}>
      <label className="cell--name">{name}</label>
      <div className="cell--header-bar" />
      {note && <CellHeaderNote note={note} />}
    </div>
  )
}

export default CellHeader
