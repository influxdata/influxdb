// Libraries
import React, {FC} from 'react'

// Styles
import 'src/notebooks/components/EmptyPipeList.scss'

const EmptyPipeList: FC = () => {
  return (
    <div className="notebook-empty">
      <div className="notebook-empty--graphic" />
      <h3>Welcome to Notebooks</h3>
      <p>
        This is a more flexible way to explore, visualize, and (eventually)
        alert on your data
      </p>
      <p>
        Get started by <strong>Adding a Cell</strong> from the top left menu
      </p>
    </div>
  )
}

export default EmptyPipeList
