import AJAX from 'src/utils/ajax'

const msToRFC = ms => ms && new Date(parseInt(ms, 10)).toISOString()
const rfcToMS = rfc3339 => rfc3339 && JSON.stringify(Date.parse(rfc3339))
const annoToMillisecond = anno => ({
  ...anno,
  startTime: rfcToMS(anno.startTime),
  endTime: rfcToMS(anno.endTime),
})
const annoToRFC = anno => ({
  ...anno,
  startTime: msToRFC(anno.startTime),
  endTime: msToRFC(anno.endTime),
})

export const createAnnotation = async (url, newAnno) => {
  const data = annoToRFC(newAnno)
  const response = await AJAX({method: 'POST', url, data})
  return annoToMillisecond(response.data)
}
