import React, {FC} from 'react'

interface Props {}

const CreateBucketFromBuilder: FC<Props> = () => {
  const handleItemClick = (): void => {
    console.log('bloop')
  }

  return (
    <div
      className="selector-list--item"
      data-testid="selector-list add-bucket"
      onClick={handleItemClick}
      title="Click to create a bucket on the fly"
    >
      Create Bucket
    </div>
  )
}

export default CreateBucketFromBuilder
