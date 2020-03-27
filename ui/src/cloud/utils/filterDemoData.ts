const DemoDataBucketNames = ['Website Monitoring Bucket']

export const isDemoData = (bucket): boolean => {
  if (DemoDataBucketNames.includes(bucket.name)) {
    //bucket is demo data bucket
    return true
  }
  return false
}
