import React, {FC, useContext} from 'react'
import {SlideToggle, InputLabel} from '@influxdata/clockface'
import {NotebookContext} from 'src/notebooks/context/notebook.current'

const PresentationMode: FC = () => {
  const {notebook, update} = useContext(NotebookContext)

  const handleChange = () => {
    update({readOnly: !notebook.readOnly})
  }

  return (
    <>
      <SlideToggle active={notebook.readOnly} onChange={handleChange} />
      <InputLabel>Presentation</InputLabel>
    </>
  )
}

export default PresentationMode
