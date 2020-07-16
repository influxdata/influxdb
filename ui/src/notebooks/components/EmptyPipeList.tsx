// Libraries
import React, {FC} from 'react'

// Components
import AddButtons from 'src/notebooks/components/AddButtons'

// Styles
import 'src/notebooks/components/EmptyPipeList.scss'

const EmptyPipeList: FC = () => {
  return (
    <div className="notebook-empty">
      <div className="notebook-empty--graphic" />
      <h3>Welcome to Flows</h3>
      <p>
        This is a more flexible way to explore, visualize, and (eventually)
        alert on your data
      </p>
      <p>
        Get started by <strong>Adding a Cell</strong> below
      </p>
      <div className="notebook-empty--buttons">
        <AddButtons eventName="Add from empty" />
      </div>
    </div>
  )
}

export default EmptyPipeList
