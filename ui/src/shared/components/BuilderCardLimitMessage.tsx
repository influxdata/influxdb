import React, {SFC} from 'react'

interface Props {
  itemCount: number
  limitCount: number
}

const BuilderCardLimitMessage: SFC<Props> = ({itemCount, limitCount}) => {
  if (itemCount < limitCount) {
    return null
  }

  return (
    <div className="builder-card-limit-message">
      Showing first {limitCount} results. Use the search bar to find more.
    </div>
  )
}

export default BuilderCardLimitMessage
